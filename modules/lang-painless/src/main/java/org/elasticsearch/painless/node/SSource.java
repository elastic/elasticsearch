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
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.MethodKey;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptInterface;
import org.elasticsearch.painless.SimpleChecksAdapter;
import org.elasticsearch.painless.WriterConstants;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.Printer;
import org.objectweb.asm.util.TraceClassVisitor;

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
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.painless.WriterConstants.BASE_CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.BOOTSTRAP_METHOD_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.COLLECTIONS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONSTRUCTOR;
import static org.elasticsearch.painless.WriterConstants.CONVERT_TO_SCRIPT_EXCEPTION_METHOD;
import static org.elasticsearch.painless.WriterConstants.EMPTY_MAP_METHOD;
import static org.elasticsearch.painless.WriterConstants.EXCEPTION_TYPE;
import static org.elasticsearch.painless.WriterConstants.OUT_OF_MEMORY_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STACK_OVERFLOW_ERROR_TYPE;

/**
 * The root of all Painless trees.  Contains a series of statements.
 */
public final class SSource extends AStatement {

    /**
     * Tracks derived arguments and the loop counter.  Must be given to any source of input
     * prior to beginning the analysis phase so that reserved variables
     * are known ahead of time to assign appropriate slots without
     * being wasteful.
     */
    public interface Reserved {
        void markUsedVariable(String name);

        void setMaxLoopCounter(int max);
        int getMaxLoopCounter();
    }

    public static final class MainMethodReserved implements Reserved {
        private final Set<String> usedVariables = new HashSet<>();
        private int maxLoopCounter = 0;

        @Override
        public void markUsedVariable(String name) {
            usedVariables.add(name);
        }

        @Override
        public void setMaxLoopCounter(int max) {
            maxLoopCounter = max;
        }

        @Override
        public int getMaxLoopCounter() {
            return maxLoopCounter;
        }

        public Set<String> getUsedVariables() {
            return unmodifiableSet(usedVariables);
        }
    }

    private final ScriptInterface scriptInterface;
    private final CompilerSettings settings;
    private final String name;
    private final String source;
    private final Printer debugStream;
    private final MainMethodReserved reserved;
    private final List<SFunction> functions;
    private final Globals globals;
    private final List<AStatement> statements;

    private Locals mainMethod;
    private byte[] bytes;

    public SSource(ScriptInterface scriptInterface, CompilerSettings settings, String name, String source, Printer debugStream,
                   MainMethodReserved reserved, Location location,
                   List<SFunction> functions, Globals globals, List<AStatement> statements) {
        super(location);
        this.scriptInterface = Objects.requireNonNull(scriptInterface);
        this.settings = Objects.requireNonNull(settings);
        this.name = Objects.requireNonNull(name);
        this.source = Objects.requireNonNull(source);
        this.debugStream = debugStream;
        this.reserved = Objects.requireNonNull(reserved);
        // process any synthetic functions generated by walker (because right now, thats still easy)
        functions.addAll(globals.getSyntheticMethods().values());
        globals.getSyntheticMethods().clear();
        this.functions = Collections.unmodifiableList(functions);
        this.statements = Collections.unmodifiableList(statements);
        this.globals = globals;
    }

    @Override
    void extractVariables(Set<String> variables) {
        // we should never be extracting from a function, as functions are top-level!
        throw new IllegalStateException("Illegal tree structure.");
    }

    public void analyze() {
        Map<MethodKey, Method> methods = new HashMap<>();

        for (SFunction function : functions) {
            function.generateSignature();

            MethodKey key = new MethodKey(function.name, function.parameters.size());

            if (methods.put(key, function.method) != null) {
                throw createError(new IllegalArgumentException("Duplicate functions with name [" + function.name + "]."));
            }
        }

        analyze(Locals.newProgramScope(methods.values()));
    }

    @Override
    void analyze(Locals program) {
        for (SFunction function : functions) {
            Locals functionLocals = Locals.newFunctionScope(program, function.rtnType, function.parameters,
                                                            function.reserved.getMaxLoopCounter());
            function.analyze(functionLocals);
        }

        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty script."));
        }

        mainMethod = Locals.newMainMethodScope(scriptInterface, program, reserved.getMaxLoopCounter());

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

    public void write() {
        // Create the ClassWriter.

        int classFrames = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String classBase = BASE_CLASS_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String classInterfaces[] = new String[] { Type.getType(scriptInterface.getInterface()).getInternalName() };

        ClassWriter writer = new ClassWriter(classFrames);
        ClassVisitor visitor = writer;

        // if picky is enabled, turn on some checks. instead of VerifyError at the end, you get a helpful stacktrace.
        if (settings.isPicky()) {
            visitor = new SimpleChecksAdapter(visitor);
        }

        if (debugStream != null) {
            visitor = new TraceClassVisitor(visitor, debugStream, null);
        }
        visitor.visit(WriterConstants.CLASS_VERSION, classAccess, className, null, classBase, classInterfaces);
        visitor.visitSource(Location.computeSourceName(name, source), null);

        // Write the constructor:
        MethodWriter constructor = new MethodWriter(Opcodes.ACC_PUBLIC, CONSTRUCTOR, visitor, globals.getStatements(), settings);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(BASE_CLASS_TYPE, CONSTRUCTOR);
        constructor.returnValue();
        constructor.endMethod();

        // Write the method defined in the interface:
        MethodWriter executeMethod = new MethodWriter(Opcodes.ACC_PUBLIC, scriptInterface.getExecuteMethod(), visitor,
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

        // Write any uses$varName methods for used variables
        for (org.objectweb.asm.commons.Method usesMethod : scriptInterface.getUsesMethods()) {
            MethodWriter ifaceMethod = new MethodWriter(Opcodes.ACC_PUBLIC, usesMethod, visitor, globals.getStatements(), settings);
            ifaceMethod.visitCode();
            ifaceMethod.push(reserved.getUsedVariables().contains(usesMethod.getName().substring("uses$".length())));
            ifaceMethod.returnValue();
            ifaceMethod.endMethod();
        }

        // End writing the class and store the generated bytes.

        visitor.visitEnd();
        bytes = writer.toByteArray();
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

        if (reserved.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = mainMethod.getVariable(null, Locals.LOOP);

            writer.push(reserved.getMaxLoopCounter());
            writer.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        for (AStatement statement : statements) {
            statement.write(writer, globals);
        }

        if (!methodEscape) {
            switch (scriptInterface.getExecuteMethod().getReturnType().getSort()) {
            case org.objectweb.asm.Type.VOID:    break;
            case org.objectweb.asm.Type.BOOLEAN: writer.push(false); break;
            case org.objectweb.asm.Type.BYTE:    writer.push(0); break;
            case org.objectweb.asm.Type.SHORT:   writer.push(0); break;
            case org.objectweb.asm.Type.INT:     writer.push(0); break;
            case org.objectweb.asm.Type.LONG:    writer.push(0L); break;
            case org.objectweb.asm.Type.FLOAT:   writer.push(0f); break;
            case org.objectweb.asm.Type.DOUBLE:  writer.push(0d); break;
            default:                             writer.visitInsn(Opcodes.ACONST_NULL);
            }
            writer.returnValue();
        }

        writer.mark(endTry);
        writer.goTo(endCatch);
        // This looks like:
        // } catch (PainlessExplainError e) {
        //   throw this.convertToScriptException(e, e.getHeaders())
        // }
        writer.visitTryCatchBlock(startTry, endTry, startExplainCatch, PAINLESS_EXPLAIN_ERROR_TYPE.getInternalName());
        writer.mark(startExplainCatch);
        writer.loadThis();
        writer.swap();
        writer.dup();
        writer.invokeVirtual(PAINLESS_EXPLAIN_ERROR_TYPE, PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD);
        writer.invokeVirtual(BASE_CLASS_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
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
        writer.invokeVirtual(BASE_CLASS_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
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
