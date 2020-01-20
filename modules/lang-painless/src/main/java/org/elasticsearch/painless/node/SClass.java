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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Constant;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.Printer;

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
import static org.elasticsearch.painless.WriterConstants.FUNCTION_TABLE_TYPE;
import static org.elasticsearch.painless.WriterConstants.GET_NAME_METHOD;
import static org.elasticsearch.painless.WriterConstants.GET_SOURCE_METHOD;
import static org.elasticsearch.painless.WriterConstants.GET_STATEMENTS_METHOD;
import static org.elasticsearch.painless.WriterConstants.OUT_OF_MEMORY_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STACK_OVERFLOW_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRING_TYPE;

/**
 * The root of all Painless trees.  Contains a series of statements.
 */
public final class SClass extends AStatement {

    private final ScriptClassInfo scriptClassInfo;
    private final String name;
    private final Printer debugStream;
    private final List<SFunction> functions = new ArrayList<>();
    private final List<SField> fields = new ArrayList<>();
    private final Globals globals;
    private final List<AStatement> statements;

    private CompilerSettings settings;

    private ScriptRoot table;
    private Locals mainMethod;
    private final Set<String> extractedVariables;
    private final List<org.objectweb.asm.commons.Method> getMethods;
    private byte[] bytes;

    public SClass(ScriptClassInfo scriptClassInfo, String name, String sourceText, Printer debugStream,
            Location location, List<SFunction> functions, List<AStatement> statements) {
        super(location);
        this.scriptClassInfo = Objects.requireNonNull(scriptClassInfo);
        this.name = Objects.requireNonNull(name);
        this.debugStream = debugStream;
        this.functions.addAll(Objects.requireNonNull(functions));
        this.statements = Collections.unmodifiableList(statements);
        this.globals = new Globals(new BitSet(sourceText.length()));

        this.extractedVariables = new HashSet<>();
        this.getMethods = new ArrayList<>();
    }

    void addFunction(SFunction function) {
        functions.add(function);
    }

    void addField(SField field) {
        fields.add(field);
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

    public ScriptRoot analyze(PainlessLookup painlessLookup, CompilerSettings settings) {
        this.settings = settings;
        table = new ScriptRoot(painlessLookup, settings, scriptClassInfo, this);

        for (SFunction function : functions) {
            function.generateSignature(painlessLookup);

            String key = FunctionTable.buildLocalFunctionKey(function.name, function.parameters.size());

            if (table.getFunctionTable().getFunction(key) != null) {
                throw createError(new IllegalArgumentException("Illegal duplicate functions [" + key + "]."));
            }

            table.getFunctionTable().addFunction(function.name, function.returnType, function.typeParameters, false);
        }

        Locals locals = Locals.newProgramScope();
        analyze(table, locals);
        return table;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals program) {
        // copy protection is required because synthetic functions are
        // added for lambdas/method references and analysis here is
        // only for user-defined functions
        List<SFunction> functions = new ArrayList<>(this.functions);
        for (SFunction function : functions) {
            Locals functionLocals =
                Locals.newFunctionScope(program, function.returnType, function.parameters, settings.getMaxLoopCounter());
            function.analyze(scriptRoot, functionLocals);
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
            statement.analyze(scriptRoot, mainMethod);

            methodEscape = statement.methodEscape;
            allEscape = statement.allEscape;
        }
    }

    public Map<String, Object> write() {
        // Create the ClassWriter.

        int classFrames = org.objectweb.asm.ClassWriter.COMPUTE_FRAMES | org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String[] classInterfaces = new String[] { interfaceBase };

        ClassWriter classWriter = new ClassWriter(settings, globals.getStatements(), debugStream,
                scriptClassInfo.getBaseClass(), classFrames, classAccess, className, classInterfaces);
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(name), null);

        // Write the a method to bootstrap def calls
        MethodWriter bootstrapDef = classWriter.newMethodWriter(Opcodes.ACC_STATIC | Opcodes.ACC_VARARGS, DEF_BOOTSTRAP_METHOD);
        bootstrapDef.visitCode();
        bootstrapDef.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
        bootstrapDef.getStatic(CLASS_TYPE, "$FUNCTIONS", FUNCTION_TABLE_TYPE);
        bootstrapDef.loadArgs();
        bootstrapDef.invokeStatic(DEF_BOOTSTRAP_DELEGATE_TYPE, DEF_BOOTSTRAP_DELEGATE_METHOD);
        bootstrapDef.returnValue();
        bootstrapDef.endMethod();

        // Write static variables for name, source and statements used for writing exception messages
        classVisitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$NAME", STRING_TYPE.getDescriptor(), null, null).visitEnd();
        classVisitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$SOURCE", STRING_TYPE.getDescriptor(), null, null).visitEnd();
        classVisitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$STATEMENTS", BITSET_TYPE.getDescriptor(), null, null).visitEnd();

        // Write the static variables used by the method to bootstrap def calls
        classVisitor.visitField(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$DEFINITION", DEFINITION_TYPE.getDescriptor(), null, null).visitEnd();
        classVisitor.visitField(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$FUNCTIONS", FUNCTION_TABLE_TYPE.getDescriptor(), null, null).visitEnd();

        org.objectweb.asm.commons.Method init;

        if (scriptClassInfo.getBaseClass().getConstructors().length == 0) {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class,
                scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes()).toMethodDescriptorString());
        }

        // Write the constructor:
        MethodWriter constructor = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, init);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        constructor.returnValue();
        constructor.endMethod();

        // Write a method to get static variable source
        MethodWriter nameMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, GET_NAME_METHOD);
        nameMethod.visitCode();
        nameMethod.getStatic(CLASS_TYPE, "$NAME", STRING_TYPE);
        nameMethod.returnValue();
        nameMethod.endMethod();

        // Write a method to get static variable source
        MethodWriter sourceMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, GET_SOURCE_METHOD);
        sourceMethod.visitCode();
        sourceMethod.getStatic(CLASS_TYPE, "$SOURCE", STRING_TYPE);
        sourceMethod.returnValue();
        sourceMethod.endMethod();

        // Write a method to get static variable statements
        MethodWriter statementsMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, GET_STATEMENTS_METHOD);
        statementsMethod.visitCode();
        statementsMethod.getStatic(CLASS_TYPE, "$STATEMENTS", BITSET_TYPE);
        statementsMethod.returnValue();
        statementsMethod.endMethod();

        // Write the method defined in the interface:
        MethodWriter executeMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, scriptClassInfo.getExecuteMethod());
        executeMethod.visitCode();
        write(classWriter, executeMethod, globals);
        executeMethod.endMethod();

        // Write all functions:
        for (SFunction function : functions) {
            function.write(classWriter, globals);
        }

        // Write all fields:
        for (SField field : fields) {
            field.write(classWriter);
        }

        // Write the constants
        if (false == globals.getConstantInitializers().isEmpty()) {
            Collection<Constant> inits = globals.getConstantInitializers().values();

            // Initialize the constants in a static initializer
            final MethodWriter clinit = new MethodWriter(Opcodes.ACC_STATIC,
                    WriterConstants.CLINIT, classVisitor, globals.getStatements(), settings);
            clinit.visitCode();
            for (Constant constant : inits) {
                constant.initializer.accept(clinit);
                clinit.putStatic(CLASS_TYPE, constant.name, constant.type);
            }
            clinit.returnValue();
            clinit.endMethod();
        }

        // Write any needsVarName methods for used variables
        for (org.objectweb.asm.commons.Method needsMethod : scriptClassInfo.getNeedsMethods()) {
            String name = needsMethod.getName();
            name = name.substring(5);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            MethodWriter ifaceMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, needsMethod);
            ifaceMethod.visitCode();
            ifaceMethod.push(extractedVariables.contains(name));
            ifaceMethod.returnValue();
            ifaceMethod.endMethod();
        }

        // End writing the class and store the generated bytes.

        classVisitor.visitEnd();
        bytes = classWriter.getClassBytes();

        Map<String, Object> statics = new HashMap<>();
        statics.put("$FUNCTIONS", table.getFunctionTable());

        for (SField field : fields) {
            if (field.getInstance() != null) {
                statics.put(field.getName(), field.getInstance());
            }
        }

        return statics;
    }

    @Override
    void write(org.elasticsearch.painless.ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        // We wrap the whole method in a few try/catches to handle and/or convert other exceptions to ScriptException
        Label startTry = new Label();
        Label endTry = new Label();
        Label startExplainCatch = new Label();
        Label startOtherCatch = new Label();
        Label endCatch = new Label();
        methodWriter.mark(startTry);

        if (settings.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = mainMethod.getVariable(null, Locals.LOOP);

            methodWriter.push(settings.getMaxLoopCounter());
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        for (org.objectweb.asm.commons.Method method : getMethods) {
            String name = method.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            Variable variable = mainMethod.getVariable(null, name);

            methodWriter.loadThis();
            methodWriter.invokeVirtual(Type.getType(scriptClassInfo.getBaseClass()), method);
            methodWriter.visitVarInsn(method.getReturnType().getOpcode(Opcodes.ISTORE), variable.getSlot());
        }

        for (AStatement statement : statements) {
            statement.write(classWriter, methodWriter, globals);
        }
        if (!methodEscape) {
            switch (scriptClassInfo.getExecuteMethod().getReturnType().getSort()) {
                case org.objectweb.asm.Type.VOID:
                    break;
                case org.objectweb.asm.Type.BOOLEAN:
                    methodWriter.push(false);
                    break;
                case org.objectweb.asm.Type.BYTE:
                    methodWriter.push(0);
                    break;
                case org.objectweb.asm.Type.SHORT:
                    methodWriter.push(0);
                    break;
                case org.objectweb.asm.Type.INT:
                    methodWriter.push(0);
                    break;
                case org.objectweb.asm.Type.LONG:
                    methodWriter.push(0L);
                    break;
                case org.objectweb.asm.Type.FLOAT:
                    methodWriter.push(0f);
                    break;
                case org.objectweb.asm.Type.DOUBLE:
                    methodWriter.push(0d);
                    break;
                default:
                    methodWriter.visitInsn(Opcodes.ACONST_NULL);
            }
            methodWriter.returnValue();
        }

        methodWriter.mark(endTry);
        methodWriter.goTo(endCatch);
        // This looks like:
        // } catch (PainlessExplainError e) {
        //   throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
        // }
        methodWriter.visitTryCatchBlock(startTry, endTry, startExplainCatch, PAINLESS_EXPLAIN_ERROR_TYPE.getInternalName());
        methodWriter.mark(startExplainCatch);
        methodWriter.loadThis();
        methodWriter.swap();
        methodWriter.dup();
        methodWriter.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
        methodWriter.invokeVirtual(PAINLESS_EXPLAIN_ERROR_TYPE, PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD);
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
        methodWriter.throwException();
        // This looks like:
        // } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
        //   throw this.convertToScriptException(e, e.getHeaders())
        // }
        // We *think* it is ok to catch OutOfMemoryError and StackOverflowError because Painless is stateless
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, PAINLESS_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, BOOTSTRAP_METHOD_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, OUT_OF_MEMORY_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, STACK_OVERFLOW_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, EXCEPTION_TYPE.getInternalName());
        methodWriter.mark(startOtherCatch);
        methodWriter.loadThis();
        methodWriter.swap();
        methodWriter.invokeStatic(COLLECTIONS_TYPE, EMPTY_MAP_METHOD);
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
        methodWriter.throwException();
        methodWriter.mark(endCatch);
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
