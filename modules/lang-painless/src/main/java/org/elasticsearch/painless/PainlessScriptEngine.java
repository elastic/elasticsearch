/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.apache.logging.log4j.core.tools.Generate;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.Compiler.Loader;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.painless.WriterConstants.OBJECT_TYPE;
import static org.elasticsearch.painless.node.SSource.MainMethodReserved;

/**
 * Implementation of a ScriptEngine for the Painless language.
 */
public final class PainlessScriptEngine extends AbstractComponent implements ScriptEngine {

    /**
     * Standard name of the Painless language.
     */
    public static final String NAME = "painless";

    /**
     * Permissions context used during compilation.
     */
    private static final AccessControlContext COMPILATION_CONTEXT;

    /**
     * Setup the allowed permissions.
     */
    static {
        final Permissions none = new Permissions();
        none.setReadOnly();
        COMPILATION_CONTEXT = new AccessControlContext(new ProtectionDomain[] {
            new ProtectionDomain(null, none)
        });
    }

    /**
     * Default compiler settings to be used. Note that {@link CompilerSettings} is mutable but this instance shouldn't be mutated outside
     * of {@link PainlessScriptEngine#PainlessScriptEngine(Settings, Collection)}.
     */
    private final CompilerSettings defaultCompilerSettings = new CompilerSettings();

    private final Map<ScriptContext<?>, Compiler> contextsToCompilers;

    /**
     * Constructor.
     * @param settings The settings to initialize the engine with.
     */
    public PainlessScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        super(settings);

        defaultCompilerSettings.setRegexesEnabled(CompilerSettings.REGEX_ENABLED.get(settings));

        Map<ScriptContext<?>, Compiler> contextsToCompilers = new HashMap<>();

        for (ScriptContext<?> context : contexts) {
            if (context.instanceClazz.equals(SearchScript.class) || context.instanceClazz.equals(ExecutableScript.class)) {
                contextsToCompilers.put(context, new Compiler(GenericElasticsearchScript.class, Definition.DEFINITION));
            } else {
                contextsToCompilers.put(context, new Compiler(context.instanceClazz, Definition.DEFINITION));
            }
        }

        this.contextsToCompilers = Collections.unmodifiableMap(contextsToCompilers);
    }

    /**
     * Get the type name(s) for the language.
     * @return Always contains only the single name of the language.
     */
    @Override
    public String getType() {
        return NAME;
    }

    /**
     * When a script is anonymous (inline), we give it this name.
     */
    static final String INLINE_NAME = "<inline>";

    @Override
    public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
        if (context.instanceClazz.equals(SearchScript.class)) {
            GenericElasticsearchScript painlessScript =
                (GenericElasticsearchScript)compile(contextsToCompilers.get(context), scriptName, scriptSource, params);

            SearchScript.Factory factory = (p, lookup) -> new SearchScript.LeafFactory() {
                @Override
                public SearchScript newInstance(final LeafReaderContext context) {
                    return new ScriptImpl(painlessScript, p, lookup, context);
                }
                @Override
                public boolean needs_score() {
                    return painlessScript.needs_score();
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ExecutableScript.class)) {
            GenericElasticsearchScript painlessScript =
                (GenericElasticsearchScript)compile(contextsToCompilers.get(context), scriptName, scriptSource, params);

            ExecutableScript.Factory factory = (p) -> new ScriptImpl(painlessScript, p, null, null);
            return context.factoryClazz.cast(factory);
        } else {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            // Create our loader (which loads compiled code with no permissions).
            final Loader loader = AccessController.doPrivileged(new PrivilegedAction<Loader>() {
                @Override
                public Loader run() {
                    return new Loader(getClass().getClassLoader());
                }
            });

            MainMethodReserved reserved = new MainMethodReserved();
            compile(contextsToCompilers.get(context), loader, reserved, scriptName, scriptSource, params);

            if (context.statefulFactoryClazz != null) {
                return generateFactory(loader, context, reserved, generateStatefulFactory(loader, context, reserved));
            } else {
                return generateFactory(loader, context, reserved, WriterConstants.CLASS_TYPE);
            }
        }
    }

    /**
     * Generates a stateful factory class that will return script instances.  Acts as a middle man between
     * the {@link ScriptContext#factoryClazz} and the {@link ScriptContext#instanceClazz} when used so that
     * the stateless factory can be used for caching and the stateful factory can act as a cache for new
     * script instances.  Uses the newInstance method from a {@link ScriptContext#statefulFactoryClazz} to
     * define the factory method to create new instances of the {@link ScriptContext#instanceClazz}.
     * @param loader The {@link ClassLoader} that is used to define the factory class and script class.
     * @param context The {@link ScriptContext}'s semantics are used to define the factory class.
     * @param <T> The factory class.
     * @return A factory class that will return script instances.
     */
    private <T> Type generateStatefulFactory(Loader loader, ScriptContext<T> context, MainMethodReserved reserved) {
        int classFrames = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = Type.getType(context.statefulFactoryClazz).getInternalName();
        String className = interfaceBase + "$StatefulFactory";
        String classInterfaces[] = new String[] { interfaceBase };

        ClassWriter writer = new ClassWriter(classFrames);
        writer.visit(WriterConstants.CLASS_VERSION, classAccess, className, null, OBJECT_TYPE.getInternalName(), classInterfaces);

        Method newFactory = null;

        for (Method method : context.factoryClazz.getMethods()) {
            if ("newFactory".equals(method.getName())) {
                newFactory = method;

                break;
            }
        }

        for (int count = 0; count < newFactory.getParameterTypes().length; ++count) {
            writer.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "$arg" + count,
                Type.getType(newFactory.getParameterTypes()[count]).getDescriptor(), null, null).visitEnd();
        }

        org.objectweb.asm.commons.Method base =
            new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        org.objectweb.asm.commons.Method init = new org.objectweb.asm.commons.Method("<init>",
            MethodType.methodType(void.class, newFactory.getParameterTypes()).toMethodDescriptorString());

        GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ASM5, init,
            writer.visitMethod(Opcodes.ACC_PUBLIC, init.getName(), init.getDescriptor(), null, null));
        constructor.visitCode();
        constructor.loadThis();
        constructor.invokeConstructor(OBJECT_TYPE, base);

        for (int count = 0; count < newFactory.getParameterTypes().length; ++count) {
            constructor.loadThis();
            constructor.loadArg(count);
            constructor.putField(Type.getType(className), "$arg" + count, Type.getType(newFactory.getParameterTypes()[count]));
        }

        constructor.returnValue();
        constructor.endMethod();

        Method newInstance = null;

        for (Method method : context.statefulFactoryClazz.getMethods()) {
            if ("newInstance".equals(method.getName())) {
                newInstance = method;

                break;
            }
        }

        org.objectweb.asm.commons.Method instance = new org.objectweb.asm.commons.Method(newInstance.getName(),
            MethodType.methodType(newInstance.getReturnType(), newInstance.getParameterTypes()).toMethodDescriptorString());

        List<Class<?>> parameters = new ArrayList<>(Arrays.asList(newFactory.getParameterTypes()));
        parameters.addAll(Arrays.asList(newInstance.getParameterTypes()));

        org.objectweb.asm.commons.Method constru = new org.objectweb.asm.commons.Method("<init>",
            MethodType.methodType(void.class, parameters.toArray(new Class<?>[] {})).toMethodDescriptorString());

        GeneratorAdapter adapter = new GeneratorAdapter(Opcodes.ASM5, instance,
            writer.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL,
                instance.getName(), instance.getDescriptor(), null, null));
        adapter.visitCode();
        adapter.newInstance(WriterConstants.CLASS_TYPE);
        adapter.dup();

        for (int count = 0; count < newFactory.getParameterTypes().length; ++count) {
            adapter.loadThis();
            adapter.getField(Type.getType(className), "$arg" + count, Type.getType(newFactory.getParameterTypes()[count]));
        }

        adapter.loadArgs();
        adapter.invokeConstructor(WriterConstants.CLASS_TYPE, constru);
        adapter.returnValue();
        adapter.endMethod();

        writeNeedsMethods(context.statefulFactoryClazz, writer, reserved);
        writer.visitEnd();

        loader.defineFactory(className.replace('/', '.'), writer.toByteArray());

        return Type.getType(className);
    }

    /**
     * Generates a factory class that will return script instances or stateful factories.
     * Uses the newInstance method from a {@link ScriptContext#factoryClazz} to define the factory method
     * to create new instances of the {@link ScriptContext#instanceClazz} or uses the newFactory method
     * to create new factories of the {@link ScriptContext#statefulFactoryClazz}.
     * @param loader The {@link ClassLoader} that is used to define the factory class and script class.
     * @param context The {@link ScriptContext}'s semantics are used to define the factory class.
     * @param classType The type to be instaniated in the newFactory or newInstance method.  Depends
     *                  on whether a {@link ScriptContext#statefulFactoryClazz} is specified.
     * @param <T> The factory class.
     * @return A factory class that will return script instances.
     */
    private <T> T generateFactory(Loader loader, ScriptContext<T> context, MainMethodReserved reserved, Type classType) {
        int classFrames = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER| Opcodes.ACC_FINAL;
        String interfaceBase = Type.getType(context.factoryClazz).getInternalName();
        String className = interfaceBase + "$Factory";
        String classInterfaces[] = new String[] { interfaceBase };

        ClassWriter writer = new ClassWriter(classFrames);
        writer.visit(WriterConstants.CLASS_VERSION, classAccess, className, null, OBJECT_TYPE.getInternalName(), classInterfaces);

        org.objectweb.asm.commons.Method init =
            new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());

        GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ASM5, init,
                writer.visitMethod(Opcodes.ACC_PUBLIC, init.getName(), init.getDescriptor(), null, null));
        constructor.visitCode();
        constructor.loadThis();
        constructor.invokeConstructor(OBJECT_TYPE, init);
        constructor.returnValue();
        constructor.endMethod();

        Method reflect = null;

        for (Method method : context.factoryClazz.getMethods()) {
            if ("newInstance".equals(method.getName())) {
                reflect = method;

                break;
            } else if ("newFactory".equals(method.getName())) {
                reflect = method;

                break;
            }
        }

        org.objectweb.asm.commons.Method instance = new org.objectweb.asm.commons.Method(reflect.getName(),
            MethodType.methodType(reflect.getReturnType(), reflect.getParameterTypes()).toMethodDescriptorString());
        org.objectweb.asm.commons.Method constru = new org.objectweb.asm.commons.Method("<init>",
            MethodType.methodType(void.class, reflect.getParameterTypes()).toMethodDescriptorString());

        GeneratorAdapter adapter = new GeneratorAdapter(Opcodes.ASM5, instance,
                writer.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL,
                                   instance.getName(), instance.getDescriptor(), null, null));
        adapter.visitCode();
        adapter.newInstance(classType);
        adapter.dup();
        adapter.loadArgs();
        adapter.invokeConstructor(classType, constru);
        adapter.returnValue();
        adapter.endMethod();

        writeNeedsMethods(context.factoryClazz, writer, reserved);
        writer.visitEnd();

        Class<?> factory = loader.defineFactory(className.replace('/', '.'), writer.toByteArray());

        try {
            return context.factoryClazz.cast(factory.getConstructor().newInstance());
        } catch (Exception exception) { // Catch everything to let the user know this is something caused internally.
            throw new IllegalStateException(
                "An internal error occurred attempting to define the factory class [" + className + "].", exception);
        }
    }

    private void writeNeedsMethods(Class<?> clazz, ClassWriter writer, MainMethodReserved reserved) {
        for (Method method : clazz.getMethods()) {
            if (method.getName().startsWith("needs") &&
                method.getReturnType().equals(boolean.class) && method.getParameterTypes().length == 0) {
                String name = method.getName();
                name = name.substring(5);
                name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

                org.objectweb.asm.commons.Method needs = new org.objectweb.asm.commons.Method(method.getName(),
                    MethodType.methodType(boolean.class).toMethodDescriptorString());

                GeneratorAdapter adapter = new GeneratorAdapter(Opcodes.ASM5, needs,
                    writer.visitMethod(Opcodes.ACC_PUBLIC, needs.getName(), needs.getDescriptor(), null, null));
                adapter.visitCode();
                adapter.push(reserved.getUsedVariables().contains(name));
                adapter.returnValue();
                adapter.endMethod();
            }
        }
    }

    Object compile(Compiler compiler, String scriptName, String source, Map<String, String> params, Object... args) {
        final CompilerSettings compilerSettings;

        if (params.isEmpty()) {
            // Use the default settings.
            compilerSettings = defaultCompilerSettings;
        } else {
            // Use custom settings specified by params.
            compilerSettings = new CompilerSettings();

            // Except regexes enabled - this is a node level setting and can't be changed in the request.
            compilerSettings.setRegexesEnabled(defaultCompilerSettings.areRegexesEnabled());

            Map<String, String> copy = new HashMap<>(params);

            String value = copy.remove(CompilerSettings.MAX_LOOP_COUNTER);
            if (value != null) {
                compilerSettings.setMaxLoopCounter(Integer.parseInt(value));
            }

            value = copy.remove(CompilerSettings.PICKY);
            if (value != null) {
                compilerSettings.setPicky(Boolean.parseBoolean(value));
            }

            value = copy.remove(CompilerSettings.INITIAL_CALL_SITE_DEPTH);
            if (value != null) {
                compilerSettings.setInitialCallSiteDepth(Integer.parseInt(value));
            }

            value = copy.remove(CompilerSettings.REGEX_ENABLED.getKey());
            if (value != null) {
                throw new IllegalArgumentException("[painless.regex.enabled] can only be set on node startup.");
            }

            if (!copy.isEmpty()) {
                throw new IllegalArgumentException("Unrecognized compile-time parameter(s): " + copy);
            }
        }

        // Check we ourselves are not being called by unprivileged code.
        SpecialPermission.check();

        // Create our loader (which loads compiled code with no permissions).
        final Loader loader = AccessController.doPrivileged(new PrivilegedAction<Loader>() {
            @Override
            public Loader run() {
                return new Loader(getClass().getClassLoader());
            }
        });

        try {
            // Drop all permissions to actually compile the code itself.
            return AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    String name = scriptName == null ? INLINE_NAME : scriptName;
                    Constructor<?> constructor = compiler.compile(loader, new MainMethodReserved(), name, source, compilerSettings);

                    try {
                        return constructor.newInstance(args);
                    } catch (Exception exception) { // Catch everything to let the user know this is something caused internally.
                        throw new IllegalStateException(
                            "An internal error occurred attempting to define the script [" + name + "].", exception);
                    }
                }
            }, COMPILATION_CONTEXT);
        // Note that it is safe to catch any of the following errors since Painless is stateless.
        } catch (OutOfMemoryError | StackOverflowError | VerifyError | Exception e) {
            throw convertToScriptException(scriptName == null ? source : scriptName, source, e);
        }
    }

    void compile(Compiler compiler, Loader loader, MainMethodReserved reserved,
                 String scriptName, String source, Map<String, String> params) {
        final CompilerSettings compilerSettings;

        if (params.isEmpty()) {
            // Use the default settings.
            compilerSettings = defaultCompilerSettings;
        } else {
            // Use custom settings specified by params.
            compilerSettings = new CompilerSettings();

            // Except regexes enabled - this is a node level setting and can't be changed in the request.
            compilerSettings.setRegexesEnabled(defaultCompilerSettings.areRegexesEnabled());

            Map<String, String> copy = new HashMap<>(params);

            String value = copy.remove(CompilerSettings.MAX_LOOP_COUNTER);
            if (value != null) {
                compilerSettings.setMaxLoopCounter(Integer.parseInt(value));
            }

            value = copy.remove(CompilerSettings.PICKY);
            if (value != null) {
                compilerSettings.setPicky(Boolean.parseBoolean(value));
            }

            value = copy.remove(CompilerSettings.INITIAL_CALL_SITE_DEPTH);
            if (value != null) {
                compilerSettings.setInitialCallSiteDepth(Integer.parseInt(value));
            }

            value = copy.remove(CompilerSettings.REGEX_ENABLED.getKey());
            if (value != null) {
                throw new IllegalArgumentException("[painless.regex.enabled] can only be set on node startup.");
            }

            if (!copy.isEmpty()) {
                throw new IllegalArgumentException("Unrecognized compile-time parameter(s): " + copy);
            }
        }

        try {
            // Drop all permissions to actually compile the code itself.
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    String name = scriptName == null ? INLINE_NAME : scriptName;
                    compiler.compile(loader, reserved, name, source, compilerSettings);

                    return null;
                }
            }, COMPILATION_CONTEXT);
            // Note that it is safe to catch any of the following errors since Painless is stateless.
        } catch (OutOfMemoryError | StackOverflowError | VerifyError | Exception e) {
            throw convertToScriptException(scriptName == null ? source : scriptName, source, e);
        }
    }

    private ScriptException convertToScriptException(String scriptName, String scriptSource, Throwable t) {
        // create a script stack: this is just the script portion
        List<String> scriptStack = new ArrayList<>();
        for (StackTraceElement element : t.getStackTrace()) {
            if (WriterConstants.CLASS_NAME.equals(element.getClassName())) {
                // found the script portion
                int offset = element.getLineNumber();
                if (offset == -1) {
                    scriptStack.add("<<< unknown portion of script >>>");
                } else {
                    offset--; // offset is 1 based, line numbers must be!
                    int startOffset = getPreviousStatement(scriptSource, offset);
                    int endOffset = getNextStatement(scriptSource, offset);
                    StringBuilder snippet = new StringBuilder();
                    if (startOffset > 0) {
                        snippet.append("... ");
                    }
                    snippet.append(scriptSource.substring(startOffset, endOffset));
                    if (endOffset < scriptSource.length()) {
                        snippet.append(" ...");
                    }
                    scriptStack.add(snippet.toString());
                    StringBuilder pointer = new StringBuilder();
                    if (startOffset > 0) {
                        pointer.append("    ");
                    }
                    for (int i = startOffset; i < offset; i++) {
                        pointer.append(' ');
                    }
                    pointer.append("^---- HERE");
                    scriptStack.add(pointer.toString());
                }
                break;
            }
        }
        throw new ScriptException("compile error", t, scriptStack, scriptSource, PainlessScriptEngine.NAME);
    }

    // very simple heuristic: +/- 25 chars. can be improved later.
    private int getPreviousStatement(String scriptSource, int offset) {
        return Math.max(0, offset - 25);
    }

    private int getNextStatement(String scriptSource, int offset) {
        return Math.min(scriptSource.length(), offset + 25);
    }
}
