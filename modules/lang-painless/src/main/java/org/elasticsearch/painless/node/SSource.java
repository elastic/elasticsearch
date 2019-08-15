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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Constant;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.LocalMethod;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.SimpleChecksAdapter;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.Printer;
import org.objectweb.asm.util.TraceClassVisitor;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.painless.WriterConstants.BASE_INTERFACE_TYPE;
import static org.elasticsearch.painless.WriterConstants.BITSET_TYPE;
import static org.elasticsearch.painless.WriterConstants.BOOTSTRAP_METHOD_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.COLLECTIONS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONVERT_TO_SCRIPT_EXCEPTION_METHOD;
import static org.elasticsearch.painless.WriterConstants.DEFINITION_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_DELEGATE_METHOD;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_DELEGATE_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_METHOD;
import static org.elasticsearch.painless.WriterConstants.EMPTY_MAP_METHOD;
import static org.elasticsearch.painless.WriterConstants.EXCEPTION_TYPE;
import static org.elasticsearch.painless.WriterConstants.GET_NAME_METHOD;
import static org.elasticsearch.painless.WriterConstants.GET_SOURCE_METHOD;
import static org.elasticsearch.painless.WriterConstants.GET_STATEMENTS_METHOD;
import static org.elasticsearch.painless.WriterConstants.MAP_TYPE;
import static org.elasticsearch.painless.WriterConstants.OUT_OF_MEMORY_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STACK_OVERFLOW_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRING_TYPE;

/**
 * The root of all Painless trees.  Contains a series of statements.
 */
public final class SSource extends AStatement {

    private final ScriptClassInfo scriptClassInfo;
    private final String name;
    private final Printer debugStream;
    private final List<SFunction> functions;
    private final Globals globals;
    private final List<AStatement> statements;

    private CompilerSettings settings;

    private Locals mainMethod;
    private final Set<String> extractedVariables;
    private final List<org.objectweb.asm.commons.Method> getMethods;
    private byte[] bytes;

    public SSource(ScriptClassInfo scriptClassInfo, String name, String sourceText, Printer debugStream,
            Location location, List<SFunction> functions, List<AStatement> statements) {
        super(location);
        this.scriptClassInfo = Objects.requireNonNull(scriptClassInfo);
        this.name = Objects.requireNonNull(name);
        this.debugStream = debugStream;
        this.functions = Collections.unmodifiableList(functions);
        this.statements = Collections.unmodifiableList(statements);
        this.globals = new Globals(new BitSet(sourceText.length()));

        this.extractedVariables = new HashSet<>();
        this.getMethods = new ArrayList<>();
    }

    @Override
    public void storeSettings(CompilerSettings settings) {
        for (SFunction function : functions) {
            function.storeSettings(settings);
        }

        for (AStatement statement : statements) {
            statement.storeSettings(settings);
        }

        this.settings = settings;
    }

    @Override
    public void extractVariables(Set<String> variables) {
        for (SFunction function : functions) {
            function.extractVariables(null);
        }

        for (AStatement statement : statements) {
            statement.extractVariables(variables);
        }

        extractedVariables.addAll(variables);
    }

    public void analyze(PainlessLookup painlessLookup) {
        Map<String, LocalMethod> methods = new HashMap<>();

        for (SFunction function : functions) {
            function.generateSignature(painlessLookup);

            String key = Locals.buildLocalMethodKey(function.name, function.parameters.size());

            if (methods.put(key,
                    new LocalMethod(function.name, function.returnType, function.typeParameters, function.methodType)) != null) {
                throw createError(new IllegalArgumentException("Duplicate functions with name [" + function.name + "]."));
            }
        }

        Locals locals = Locals.newProgramScope(scriptClassInfo, painlessLookup, methods.values());
        analyze(locals);
    }

    @Override
    void analyze(Locals program) {
        for (SFunction function : functions) {
            Locals functionLocals =
                Locals.newFunctionScope(program, function.returnType, function.parameters, settings.getMaxLoopCounter());
            function.analyze(functionLocals);
        }

        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty script."));
        }

        mainMethod = Locals.newMainMethodScope(scriptClassInfo, program, settings.getMaxLoopCounter());

        for (int get = 0; get < scriptClassInfo.getGetMethods().size(); ++get) {
            org.objectweb.asm.commons.Method method = scriptClassInfo.getGetMethods().get(get);
            String name = method.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            if (extractedVariables.contains(name)) {
                Class<?> rtn = scriptClassInfo.getGetReturns().get(get);
                mainMethod.addVariable(new Location("getter [" + name + "]", 0), rtn, name, true);
                getMethods.add(method);
            }
        }

        AStatement last = statements.get(statements.size() - 1);

        for (AStatement statement : statements) {
            // Note that we do not need to check after the last statement because
            // there is no statement that can be unreachable after the last.
            if (allEscape) {
                throw createError(new IllegalArgumentException("Unreachable statement."));
            }

            statement.lastSource = statement == last;

            statement.analyze(mainMethod);

            methodEscape = statement.methodEscape;
            allEscape = statement.allEscape;
        }
    }

    public Map<String, Object> write() {
        // Create the ClassWriter.

        int classFrames = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String classInterfaces[] = new String[] { interfaceBase };

        ClassWriter writer = new ClassWriter(classFrames);
        ClassVisitor visitor = writer;

        // if picky is enabled, turn on some checks. instead of VerifyError at the end, you get a helpful stacktrace.
        if (settings.isPicky()) {
            visitor = new SimpleChecksAdapter(visitor);
        }

        if (debugStream != null) {
            visitor = new TraceClassVisitor(visitor, debugStream, null);
        }
        visitor.visit(WriterConstants.CLASS_VERSION, classAccess, className, null,
            Type.getType(scriptClassInfo.getBaseClass()).getInternalName(), classInterfaces);
        visitor.visitSource(Location.computeSourceName(name), null);

        // Write the a method to bootstrap def calls
        MethodWriter bootstrapDef = new MethodWriter(Opcodes.ACC_STATIC | Opcodes.ACC_VARARGS, DEF_BOOTSTRAP_METHOD, visitor,
                globals.getStatements(), settings);
        bootstrapDef.visitCode();
        bootstrapDef.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
        bootstrapDef.getStatic(CLASS_TYPE, "$LOCALS", MAP_TYPE);
        bootstrapDef.loadArgs();
        bootstrapDef.invokeStatic(DEF_BOOTSTRAP_DELEGATE_TYPE, DEF_BOOTSTRAP_DELEGATE_METHOD);
        bootstrapDef.returnValue();
        bootstrapDef.endMethod();

        // Write static variables for name, source and statements used for writing exception messages
        visitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$NAME", STRING_TYPE.getDescriptor(), null, null).visitEnd();
        visitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$SOURCE", STRING_TYPE.getDescriptor(), null, null).visitEnd();
        visitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$STATEMENTS", BITSET_TYPE.getDescriptor(), null, null).visitEnd();

        // Write the static variables used by the method to bootstrap def calls
        visitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$DEFINITION", DEFINITION_TYPE.getDescriptor(), null, null).visitEnd();
        visitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$LOCALS", MAP_TYPE.getDescriptor(), null, null).visitEnd();

        org.objectweb.asm.commons.Method init;

        if (scriptClassInfo.getBaseClass().getConstructors().length == 0) {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class,
                scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes()).toMethodDescriptorString());
        }

        // Write the constructor:
        MethodWriter constructor = new MethodWriter(Opcodes.ACC_PUBLIC, init, visitor, globals.getStatements(), settings);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        constructor.returnValue();
        constructor.endMethod();

        // Write a method to get static variable source
        MethodWriter nameMethod = new MethodWriter(Opcodes.ACC_PUBLIC, GET_NAME_METHOD, visitor, globals.getStatements(), settings);
        nameMethod.visitCode();
        nameMethod.getStatic(CLASS_TYPE, "$NAME", STRING_TYPE);
        nameMethod.returnValue();
        nameMethod.endMethod();

        // Write a method to get static variable source
        MethodWriter sourceMethod = new MethodWriter(Opcodes.ACC_PUBLIC, GET_SOURCE_METHOD, visitor, globals.getStatements(), settings);
        sourceMethod.visitCode();
        sourceMethod.getStatic(CLASS_TYPE, "$SOURCE", STRING_TYPE);
        sourceMethod.returnValue();
        sourceMethod.endMethod();

        // Write a method to get static variable statements
        MethodWriter statementsMethod =
            new MethodWriter(Opcodes.ACC_PUBLIC, GET_STATEMENTS_METHOD, visitor, globals.getStatements(), settings);
        statementsMethod.visitCode();
        statementsMethod.getStatic(CLASS_TYPE, "$STATEMENTS", BITSET_TYPE);
        statementsMethod.returnValue();
        statementsMethod.endMethod();

        // Write the method defined in the interface:
        MethodWriter executeMethod = new MethodWriter(Opcodes.ACC_PUBLIC, scriptClassInfo.getExecuteMethod(), visitor,
                globals.getStatements(), settings);
        executeMethod.visitCode();
        write(executeMethod, globals);
        executeMethod.endMethod();

        // Write all functions:
        for (SFunction function : functions) {
            function.write(visitor, settings, globals);
        }

        // Write all synthetic functions. Note that this process may add more :)
        while (!globals.getSyntheticMethods().isEmpty()) {
            List<SFunction> current = new ArrayList<>(globals.getSyntheticMethods().values());
            globals.getSyntheticMethods().clear();
            for (SFunction function : current) {
                function.write(visitor, settings, globals);
            }
        }

        // Write the constants
        if (false == globals.getConstantInitializers().isEmpty()) {
            Collection<Constant> inits = globals.getConstantInitializers().values();

            // Fields
            for (Constant constant : inits) {
                visitor.visitField(
                        Opcodes.ACC_FINAL | Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC,
                        constant.name,
                        constant.type.getDescriptor(),
                        null,
                        null).visitEnd();
            }

            // Initialize the constants in a static initializer
            final MethodWriter clinit = new MethodWriter(Opcodes.ACC_STATIC,
                    WriterConstants.CLINIT, visitor, globals.getStatements(), settings);
            clinit.visitCode();
            for (Constant constant : inits) {
                constant.initializer.accept(clinit);
                clinit.putStatic(CLASS_TYPE, constant.name, constant.type);
            }
            clinit.returnValue();
            clinit.endMethod();
        }

        // Write class binding variables
        for (Map.Entry<String, Class<?>> classBinding : globals.getClassBindings().entrySet()) {
            String name = classBinding.getKey();
            String descriptor = Type.getType(classBinding.getValue()).getDescriptor();
            visitor.visitField(Opcodes.ACC_PRIVATE, name, descriptor, null, null).visitEnd();
        }

        // Write instance binding variables
        for (Map.Entry<Object, String> instanceBinding : globals.getInstanceBindings().entrySet()) {
            String name = instanceBinding.getValue();
            String descriptor = Type.getType(instanceBinding.getKey().getClass()).getDescriptor();
            visitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, name, descriptor, null, null).visitEnd();
        }

        // Write any needsVarName methods for used variables
        for (org.objectweb.asm.commons.Method needsMethod : scriptClassInfo.getNeedsMethods()) {
            String name = needsMethod.getName();
            name = name.substring(5);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            MethodWriter ifaceMethod = new MethodWriter(Opcodes.ACC_PUBLIC, needsMethod, visitor, globals.getStatements(), settings);
            ifaceMethod.visitCode();
            ifaceMethod.push(extractedVariables.contains(name));
            ifaceMethod.returnValue();
            ifaceMethod.endMethod();
        }

        // End writing the class and store the generated bytes.

        visitor.visitEnd();
        bytes = writer.toByteArray();

        Map<String, Object> statics = new HashMap<>();
        statics.put("$LOCALS", mainMethod.getMethods());

        for (Map.Entry<Object, String> instanceBinding : globals.getInstanceBindings().entrySet()) {
            statics.put(instanceBinding.getValue(), instanceBinding.getKey());
        }

        return statics;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        // We wrap the whole method in a few try/catches to handle and/or convert other exceptions to ScriptException
        Label startTry = new Label();
        Label endTry = new Label();
        Label startExplainCatch = new Label();
        Label startOtherCatch = new Label();
        Label endCatch = new Label();
        writer.mark(startTry);

        if (settings.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = mainMethod.getVariable(null, Locals.LOOP);

            writer.push(settings.getMaxLoopCounter());
            writer.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        for (org.objectweb.asm.commons.Method method : getMethods) {
            String name = method.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            Variable variable = mainMethod.getVariable(null, name);

            writer.loadThis();
            writer.invokeVirtual(Type.getType(scriptClassInfo.getBaseClass()), method);
            writer.visitVarInsn(method.getReturnType().getOpcode(Opcodes.ISTORE), variable.getSlot());
        }

        for (AStatement statement : statements) {
            statement.write(writer, globals);
        }
        if (!methodEscape) {
            switch (scriptClassInfo.getExecuteMethod().getReturnType().getSort()) {
                case org.objectweb.asm.Type.VOID:
                    break;
                case org.objectweb.asm.Type.BOOLEAN:
                    writer.push(false);
                    break;
                case org.objectweb.asm.Type.BYTE:
                    writer.push(0);
                    break;
                case org.objectweb.asm.Type.SHORT:
                    writer.push(0);
                    break;
                case org.objectweb.asm.Type.INT:
                    writer.push(0);
                    break;
                case org.objectweb.asm.Type.LONG:
                    writer.push(0L);
                    break;
                case org.objectweb.asm.Type.FLOAT:
                    writer.push(0f);
                    break;
                case org.objectweb.asm.Type.DOUBLE:
                    writer.push(0d);
                    break;
                default:
                    writer.visitInsn(Opcodes.ACONST_NULL);
            }
            writer.returnValue();
        }

        writer.mark(endTry);
        writer.goTo(endCatch);
        // This looks like:
        // } catch (PainlessExplainError e) {
        //   throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
        // }
        writer.visitTryCatchBlock(startTry, endTry, startExplainCatch, PAINLESS_EXPLAIN_ERROR_TYPE.getInternalName());
        writer.mark(startExplainCatch);
        writer.loadThis();
        writer.swap();
        writer.dup();
        writer.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
        writer.invokeVirtual(PAINLESS_EXPLAIN_ERROR_TYPE, PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD);
        writer.invokeInterface(BASE_INTERFACE_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
        writer.throwException();
        // This looks like:
        // } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
        //   throw this.convertToScriptException(e, e.getHeaders())
        // }
        // We *think* it is ok to catch OutOfMemoryError and StackOverflowError because Painless is stateless
        writer.visitTryCatchBlock(startTry, endTry, startOtherCatch, PAINLESS_ERROR_TYPE.getInternalName());
        writer.visitTryCatchBlock(startTry, endTry, startOtherCatch, BOOTSTRAP_METHOD_ERROR_TYPE.getInternalName());
        writer.visitTryCatchBlock(startTry, endTry, startOtherCatch, OUT_OF_MEMORY_ERROR_TYPE.getInternalName());
        writer.visitTryCatchBlock(startTry, endTry, startOtherCatch, STACK_OVERFLOW_ERROR_TYPE.getInternalName());
        writer.visitTryCatchBlock(startTry, endTry, startOtherCatch, EXCEPTION_TYPE.getInternalName());
        writer.mark(startOtherCatch);
        writer.loadThis();
        writer.swap();
        writer.invokeStatic(COLLECTIONS_TYPE, EMPTY_MAP_METHOD);
        writer.invokeInterface(BASE_INTERFACE_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
        writer.throwException();
        writer.mark(endCatch);
    }

    public BitSet getStatements() {
        return globals.getStatements();
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        List<Object> subs = new ArrayList<>(functions.size() + statements.size());
        subs.addAll(functions);
        subs.addAll(statements);
        return multilineToString(emptyList(), subs);
    }
}
