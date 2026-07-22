/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.core.Strings;
import org.elasticsearch.painless.AllocSizes;
import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.api.Augmentation;
import org.elasticsearch.painless.api.ValueIterator;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ContinueNode;
import org.elasticsearch.painless.ir.DeclarationBlockNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.DoWhileLoopNode;
import org.elasticsearch.painless.ir.DupNode;
import org.elasticsearch.painless.ir.ElvisNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.FlipArrayIndexNode;
import org.elasticsearch.painless.ir.FlipCollectionIndexNode;
import org.elasticsearch.painless.ir.FlipDefIndexNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.ir.InvokeCallDefNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.ir.LoadBraceDefNode;
import org.elasticsearch.painless.ir.LoadBraceNode;
import org.elasticsearch.painless.ir.LoadDotArrayLengthNode;
import org.elasticsearch.painless.ir.LoadDotDefNode;
import org.elasticsearch.painless.ir.LoadDotNode;
import org.elasticsearch.painless.ir.LoadDotShortcutNode;
import org.elasticsearch.painless.ir.LoadFieldMemberNode;
import org.elasticsearch.painless.ir.LoadListShortcutNode;
import org.elasticsearch.painless.ir.LoadMapShortcutNode;
import org.elasticsearch.painless.ir.LoadVariableNode;
import org.elasticsearch.painless.ir.MapInitializationNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.StoreBraceDefNode;
import org.elasticsearch.painless.ir.StoreBraceNode;
import org.elasticsearch.painless.ir.StoreDotDefNode;
import org.elasticsearch.painless.ir.StoreDotNode;
import org.elasticsearch.painless.ir.StoreDotShortcutNode;
import org.elasticsearch.painless.ir.StoreFieldMemberNode;
import org.elasticsearch.painless.ir.StoreListShortcutNode;
import org.elasticsearch.painless.ir.StoreMapShortcutNode;
import org.elasticsearch.painless.ir.StoreVariableNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.TypedCaptureReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.WhileLoopNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.spi.annotation.ScriptAwareAnnotation;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.IRDecorations.IRCAllEscape;
import org.elasticsearch.painless.symbol.IRDecorations.IRCCaptureBox;
import org.elasticsearch.painless.symbol.IRDecorations.IRCContinuous;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInitialize;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInstanceCancellationCheck;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInstanceCapture;
import org.elasticsearch.painless.symbol.IRDecorations.IRCScriptAware;
import org.elasticsearch.painless.symbol.IRDecorations.IRCStatic;
import org.elasticsearch.painless.symbol.IRDecorations.IRCStaticCancellationCheck;
import org.elasticsearch.painless.symbol.IRDecorations.IRCStaticScriptCapture;
import org.elasticsearch.painless.symbol.IRDecorations.IRCSynthetic;
import org.elasticsearch.painless.symbol.IRDecorations.IRCVarArgs;
import org.elasticsearch.painless.symbol.IRDecorations.IRDAllocationEstimator;
import org.elasticsearch.painless.symbol.IRDecorations.IRDArrayName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDArrayType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDBinaryType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDCaptureNames;
import org.elasticsearch.painless.symbol.IRDecorations.IRDCast;
import org.elasticsearch.painless.symbol.IRDecorations.IRDClassBinding;
import org.elasticsearch.painless.symbol.IRDecorations.IRDComparisonType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstant;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstantFieldName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstructor;
import org.elasticsearch.painless.symbol.IRDecorations.IRDDeclarationType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDDefReferenceEncoding;
import org.elasticsearch.painless.symbol.IRDecorations.IRDDepth;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExceptionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDField;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFieldType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFlags;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFunction;
import org.elasticsearch.painless.symbol.IRDecorations.IRDIndexName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDIndexType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDIndexedType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDInstanceBinding;
import org.elasticsearch.painless.symbol.IRDecorations.IRDInstanceType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDIterableName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDIterableType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDMaxAllocationBytes;
import org.elasticsearch.painless.symbol.IRDecorations.IRDMaxLoopCounter;
import org.elasticsearch.painless.symbol.IRDecorations.IRDMethod;
import org.elasticsearch.painless.symbol.IRDecorations.IRDModifiers;
import org.elasticsearch.painless.symbol.IRDecorations.IRDName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDOperation;
import org.elasticsearch.painless.symbol.IRDecorations.IRDParameterNames;
import org.elasticsearch.painless.symbol.IRDecorations.IRDReference;
import org.elasticsearch.painless.symbol.IRDecorations.IRDRegexLimit;
import org.elasticsearch.painless.symbol.IRDecorations.IRDReturnType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDShiftType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDSize;
import org.elasticsearch.painless.symbol.IRDecorations.IRDStoreType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDSymbol;
import org.elasticsearch.painless.symbol.IRDecorations.IRDThisMethod;
import org.elasticsearch.painless.symbol.IRDecorations.IRDTypeParameters;
import org.elasticsearch.painless.symbol.IRDecorations.IRDUnaryType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDValue;
import org.elasticsearch.painless.symbol.IRDecorations.IRDVariableName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDVariableType;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.WriteScope;
import org.elasticsearch.painless.symbol.WriteScope.Variable;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.util.Printer;

import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;

import static org.elasticsearch.painless.WriterConstants.BASE_INTERFACE_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.EQUALS;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_HASNEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_NEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_TYPE;
import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_BYTE;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_CHAR;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_FLOAT;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_INT;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_LONG;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_NEXT_SHORT;
import static org.elasticsearch.painless.WriterConstants.VALUE_ITERATOR_TYPE;

public class DefaultIRTreeToASMBytesPhase implements IRTreeVisitor<WriteScope> {

    protected void visit(IRNode irNode, WriteScope writeScope) {
        irNode.visit(this, writeScope);
    }

    public void visitScript(ClassNode irClassNode) {
        WriteScope writeScope = WriteScope.newScriptScope();
        visitClass(irClassNode, writeScope);
    }

    @Override
    public void visitClass(ClassNode irClassNode, WriteScope writeScope) {
        ScriptScope scriptScope = irClassNode.getScriptScope();
        ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();
        BitSet statements = new BitSet(scriptScope.getScriptSource().length());
        scriptScope.addStaticConstant("$STATEMENTS", statements);
        Printer debugStream = irClassNode.getDebugStream();

        // Create the ClassWriter.

        int classFrames = org.objectweb.asm.ClassWriter.COMPUTE_FRAMES | org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String[] classInterfaces = new String[] { interfaceBase };

        ClassWriter classWriter = new ClassWriter(
            scriptScope.getCompilerSettings(),
            statements,
            debugStream,
            scriptClassInfo.getBaseClass(),
            classFrames,
            classAccess,
            className,
            classInterfaces
        );
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(scriptScope.getScriptName()), null);
        writeScope = writeScope.newClassScope(classWriter);

        Method init;

        if (scriptClassInfo.getBaseClass().getConstructors().length == 0) {
            init = new Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else {
            init = new Method(
                "<init>",
                MethodType.methodType(void.class, scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes())
                    .toMethodDescriptorString()
            );
        }

        boolean needsCancelPollField = irClassNode.getFunctionsNodes()
            .stream()
            .anyMatch(f -> f.hasCondition(IRCInstanceCancellationCheck.class));

        if (needsCancelPollField) {
            classVisitor.visitField(Opcodes.ACC_PRIVATE, WriterConstants.CANCEL_POLL_FIELD, "I", null, null).visitEnd();

            MethodWriter pollCancellation = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, WriterConstants.POLL_CANCELLATION);
            pollCancellation.visitCode();
            Label noRunnable = new Label();
            pollCancellation.loadThis();
            pollCancellation.invokeInterface(WriterConstants.BASE_INTERFACE_TYPE, WriterConstants.GET_CANCELLATION_CHECK);
            pollCancellation.dup();
            pollCancellation.visitVarInsn(Opcodes.ASTORE, 1);
            pollCancellation.ifNull(noRunnable);
            pollCancellation.writeCancellationPoll(0, 1);
            pollCancellation.mark(noRunnable);
            pollCancellation.returnValue();
            pollCancellation.endMethod();
        }

        // The per-context allocation limit (-1 when tracking is disabled) is fixed for the whole compile, so it is baked
        // directly into the generated $checkAllocBytes override rather than threaded to each call site.
        long maxAllocationBytes = scriptScope.getCompilerSettings().getMaxAllocationBytes();

        if (maxAllocationBytes > 0L) {
            // private long $allocBytes — the running heuristic allocation total, accessed only by the generated
            // $incAllocBytes/getAllocBytes/$checkAllocBytes overrides below and reset at the execute entry.
            classVisitor.visitField(Opcodes.ACC_PRIVATE, WriterConstants.ALLOC_BYTES_FIELD, "J", null, null).visitEnd();

            // public long $incAllocBytes(long bytes) { return this.$allocBytes += bytes; }
            MethodWriter incAllocBytes = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, WriterConstants.INC_ALLOC_BYTES);
            incAllocBytes.visitCode();
            incAllocBytes.loadThis();
            incAllocBytes.dup();
            incAllocBytes.getField(WriterConstants.CLASS_TYPE, WriterConstants.ALLOC_BYTES_FIELD, Type.LONG_TYPE);
            incAllocBytes.loadArg(0);
            incAllocBytes.math(MethodWriter.ADD, Type.LONG_TYPE);
            incAllocBytes.visitInsn(Opcodes.DUP2_X1);
            incAllocBytes.putField(WriterConstants.CLASS_TYPE, WriterConstants.ALLOC_BYTES_FIELD, Type.LONG_TYPE);
            incAllocBytes.returnValue();
            incAllocBytes.endMethod();

            // public long getAllocBytes() { return this.$allocBytes; }
            MethodWriter getAllocBytes = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, WriterConstants.GET_ALLOC_BYTES);
            getAllocBytes.visitCode();
            getAllocBytes.loadThis();
            getAllocBytes.getField(WriterConstants.CLASS_TYPE, WriterConstants.ALLOC_BYTES_FIELD, Type.LONG_TYPE);
            getAllocBytes.returnValue();
            getAllocBytes.endMethod();

            // public void $checkAllocBytes(long bytes) {
            // long total = this.$allocBytes += bytes;
            // if (total > <limit>) AllocationGuard.allocationLimitExceeded(bytes, total, <limit>);
            // }
            // The limit is a baked-in constant; the breach path delegates to AllocationGuard to keep this method compact.
            MethodWriter checkAllocBytes = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, WriterConstants.CHECK_ALLOC_BYTES);
            checkAllocBytes.visitCode();
            checkAllocBytes.loadThis();
            checkAllocBytes.dup();
            checkAllocBytes.getField(WriterConstants.CLASS_TYPE, WriterConstants.ALLOC_BYTES_FIELD, Type.LONG_TYPE);
            checkAllocBytes.loadArg(0);
            checkAllocBytes.math(MethodWriter.ADD, Type.LONG_TYPE);
            checkAllocBytes.visitInsn(Opcodes.DUP2_X1);
            checkAllocBytes.putField(WriterConstants.CLASS_TYPE, WriterConstants.ALLOC_BYTES_FIELD, Type.LONG_TYPE);
            int totalSlot = checkAllocBytes.newLocal(Type.LONG_TYPE);
            checkAllocBytes.storeLocal(totalSlot);
            Label withinLimit = checkAllocBytes.newLabel();
            checkAllocBytes.loadLocal(totalSlot);
            checkAllocBytes.push(maxAllocationBytes);
            checkAllocBytes.ifCmp(Type.LONG_TYPE, MethodWriter.LE, withinLimit);
            checkAllocBytes.loadArg(0);
            checkAllocBytes.loadLocal(totalSlot);
            checkAllocBytes.push(maxAllocationBytes);
            checkAllocBytes.invokeStatic(WriterConstants.ALLOCATION_GUARD_TYPE, WriterConstants.ALLOCATION_LIMIT_EXCEEDED);
            checkAllocBytes.mark(withinLimit);
            checkAllocBytes.returnValue();
            checkAllocBytes.endMethod();
        }

        // Write the constructor:
        MethodWriter constructor = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, init);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        if (needsCancelPollField) {
            constructor.loadThis();
            constructor.push(WriterConstants.CANCELLATION_POLL_INTERVAL);
            constructor.putField(WriterConstants.CLASS_TYPE, WriterConstants.CANCEL_POLL_FIELD, Type.INT_TYPE);
        }
        constructor.returnValue();
        constructor.endMethod();

        BlockNode irClinitBlockNode = irClassNode.getClinitBlockNode();

        if (irClinitBlockNode.getStatementsNodes().isEmpty() == false) {
            MethodWriter methodWriter = classWriter.newMethodWriter(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                new Method("<clinit>", Type.getType(void.class), new Type[0])
            );
            visit(irClinitBlockNode, writeScope.newMethodScope(methodWriter).newBlockScope());
            methodWriter.returnValue();
            methodWriter.endMethod();
        }

        // Write all fields:
        for (FieldNode irFieldNode : irClassNode.getFieldsNodes()) {
            visit(irFieldNode, writeScope);
        }

        // Write all functions:
        for (FunctionNode irFunctionNode : irClassNode.getFunctionsNodes()) {
            visit(irFunctionNode, writeScope);
        }

        // End writing the class and store the generated bytes.
        classVisitor.visitEnd();
        irClassNode.setBytes(classWriter.getClassBytes());
    }

    @Override
    public void visitFunction(FunctionNode irFunctionNode, WriteScope writeScope) {
        int access = Opcodes.ACC_PUBLIC;

        if (irFunctionNode.hasCondition(IRCStatic.class)) {
            access |= Opcodes.ACC_STATIC;
        }

        if (irFunctionNode.hasCondition(IRCVarArgs.class)) {
            access |= Opcodes.ACC_VARARGS;
        }

        if (irFunctionNode.hasCondition(IRCSynthetic.class)) {
            access |= Opcodes.ACC_SYNTHETIC;
        }

        Type asmReturnType = MethodWriter.getType(irFunctionNode.getDecorationValue(IRDReturnType.class));
        List<Class<?>> typeParameters = irFunctionNode.getDecorationValue(IRDTypeParameters.class);
        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }

        Method method = new Method(irFunctionNode.getDecorationValue(IRDName.class), asmReturnType, asmParameterTypes);

        ClassWriter classWriter = writeScope.getClassWriter();
        MethodWriter methodWriter = classWriter.newMethodWriter(access, method);
        writeScope = writeScope.newMethodScope(methodWriter);

        if (irFunctionNode.hasCondition(IRCStatic.class) == false) {
            writeScope.defineInternalVariable(Object.class, "this");
        }

        List<String> parameterNames = irFunctionNode.getDecorationValue(IRDParameterNames.class);

        for (int index = 0; index < typeParameters.size(); ++index) {
            writeScope.defineVariable(typeParameters.get(index), parameterNames.get(index));
        }

        methodWriter.visitCode();

        boolean instanceCancellation = irFunctionNode.hasCondition(IRCInstanceCancellationCheck.class);
        boolean staticCancellation = irFunctionNode.hasCondition(IRCStaticCancellationCheck.class);
        boolean staticScriptCapture = irFunctionNode.hasCondition(IRCStaticScriptCapture.class);
        boolean hasThis = irFunctionNode.hasCondition(IRCStatic.class) == false;
        long maxAllocationBytes = irFunctionNode.getDecorationValueOrDefault(IRDMaxAllocationBytes.class, -1L);
        int maxLoopCounter = irFunctionNode.getDecorationValue(IRDMaxLoopCounter.class);

        // Define #scriptThis (= `this`) for instance functions under cancellation or tracking, so a nested static lambda
        // can capture it at its construction site. Static lambdas instead receive it as parameter 0.
        if (hasThis && (instanceCancellation || maxAllocationBytes > 0L)) {
            Variable scriptThis = writeScope.defineInternalVariable(Object.class, "scriptThis");
            methodWriter.loadThis();
            methodWriter.visitVarInsn(Opcodes.ASTORE, scriptThis.getSlot());
        }

        // Cancellation entry poll via #scriptThis (defined above, or parameter 0 for static cancellation lambdas).
        if (instanceCancellation || staticCancellation) {
            Variable scriptThis = writeScope.getInternalVariable("scriptThis");
            Variable cancelRunnable = writeScope.defineInternalVariable(Runnable.class, "cancelRunnable");
            methodWriter.visitVarInsn(Opcodes.ALOAD, scriptThis.getSlot());
            methodWriter.invokeInterface(WriterConstants.BASE_INTERFACE_TYPE, WriterConstants.GET_CANCELLATION_CHECK);
            methodWriter.visitVarInsn(Opcodes.ASTORE, cancelRunnable.getSlot());

            Label skipEntry = new Label();
            methodWriter.visitVarInsn(Opcodes.ALOAD, cancelRunnable.getSlot());
            methodWriter.ifNull(skipEntry);
            methodWriter.writeCancellationPoll(scriptThis.getSlot(), cancelRunnable.getSlot());
            methodWriter.mark(skipEntry);
        }

        // Reset the per-instance allocation counter at the entry of the execute method so each execution starts fresh.
        // The entry method is the single non-static method named "execute"; user functions are mangled and lambdas are static.
        if (maxAllocationBytes > 0L && hasThis && "execute".equals(method.getName())) {
            methodWriter.loadThis();
            methodWriter.push(0L);
            methodWriter.putField(WriterConstants.CLASS_TYPE, WriterConstants.ALLOC_BYTES_FIELD, Type.LONG_TYPE);
        }

        // Define the #allocLimit marker when tracking is on and a script pointer is reachable: `this` (instance functions)
        // or the captured #scriptThis (static lambdas, see IRCStaticScriptCapture). Its presence signals allocation sites to
        // emit pre-checks (see writeAllocationCheck); the limit itself is baked into $checkAllocBytes.
        if (maxAllocationBytes > 0L && (hasThis || staticScriptCapture)) {
            Variable allocLimit = writeScope.defineInternalVariable(long.class, "allocLimit");
            methodWriter.push(maxAllocationBytes);
            methodWriter.visitVarInsn(Opcodes.LSTORE, allocLimit.getSlot());
        }

        if (maxLoopCounter > 0) {
            Variable loop = writeScope.defineInternalVariable(int.class, "loop");
            methodWriter.push(maxLoopCounter);
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        visit(irFunctionNode.getBlockNode(), writeScope.newBlockScope());

        methodWriter.endMethod();
    }

    private static void writeBranchedLoopGuard(WriteScope writeScope, MethodWriter methodWriter, Location location, boolean legacy) {
        Variable cancelRunnable = writeScope.getInternalVariable("cancelRunnable");
        Variable loop = writeScope.getInternalVariable("loop");

        if (cancelRunnable == null) {
            if (legacy && loop != null) {
                methodWriter.writeLoopCounter(loop.getSlot(), location);
            }
            return;
        }

        if (loop == null) {
            Label skip = new Label();
            methodWriter.visitVarInsn(Opcodes.ALOAD, cancelRunnable.getSlot());
            methodWriter.ifNull(skip);
            methodWriter.writeCancellationPoll(writeScope.getInternalVariable("scriptThis").getSlot(), cancelRunnable.getSlot());
            methodWriter.mark(skip);
            return;
        }

        Label legacyPath = new Label();
        Label end = new Label();

        methodWriter.visitVarInsn(Opcodes.ALOAD, cancelRunnable.getSlot());
        methodWriter.ifNull(legacyPath);
        methodWriter.writeCancellationPoll(writeScope.getInternalVariable("scriptThis").getSlot(), cancelRunnable.getSlot());
        methodWriter.goTo(end);
        methodWriter.mark(legacyPath);
        methodWriter.writeLoopCounter(loop.getSlot(), location);
        methodWriter.mark(end);
    }

    /**
     * Emits a pre-check for a compile-time-known allocation of {@code bytes} bytes, charging the running total and tripping
     * the per-context limit before the allocating instruction executes. Does nothing when allocation tracking is inactive for
     * the enclosing function (signalled by the absence of the {@code #allocLimit} marker, e.g. tracking off or no reachable
     * script pointer). The check is a single {@code $checkAllocBytes} call on the script instance, mirroring how cancellation
     * routes through {@code _pollCancellation}: {@code this} for instance methods/instance-capturing lambdas, or the captured
     * {@code #scriptThis} for static lambdas. Net stack effect is zero, so it can be emitted directly before the allocation.
     */
    private static void writeAllocationCheck(WriteScope writeScope, long bytes) {
        if (bytes == 0 || isAllocationTrackingActive(writeScope) == false) {
            // @allocates[bytes="0"] means "audited, does not allocate" and must emit nothing.
            return;
        }

        MethodWriter methodWriter = writeScope.getMethodWriter();
        loadScriptPointer(writeScope, methodWriter);
        methodWriter.push(bytes);
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, WriterConstants.CHECK_ALLOC_BYTES);
    }

    /**
     * Spills the top {@code types.length} stack values (last type on top) into fresh locals so they can be replayed: once for
     * an {@code @allocates} estimator and once for the real call. Returns the locals in parameter order.
     */
    private static Variable[] spillCallOperands(WriteScope writeScope, MethodWriter methodWriter, String role, Class<?>[] types) {
        Variable[] operands = new Variable[types.length];

        for (int i = types.length - 1; i >= 0; --i) {
            operands[i] = writeScope.defineInternalVariable(types[i], role + i);
            methodWriter.visitVarInsn(operands[i].getAsmType().getOpcode(Opcodes.ISTORE), operands[i].getSlot());
        }

        return operands;
    }

    /** Reloads operands previously spilled by {@link #spillCallOperands} back onto the stack in parameter order. */
    private static void loadCallOperands(MethodWriter methodWriter, Variable[] operands) {
        for (Variable operand : operands) {
            methodWriter.visitVarInsn(operand.getAsmType().getOpcode(Opcodes.ILOAD), operand.getSlot());
        }
    }

    /**
     * Emits an {@code @allocates} pre-check for operands already on the stack: spill, replay through the estimator,
     * normalize via {@link org.elasticsearch.painless.AllocationGuard#sanitizeEstimate(long)}, and charge through
     * {@code $checkAllocBytes} before the allocating call executes. The caller reloads the returned operands via
     * {@link #loadCallOperands} for the real call, and must check {@link #isAllocationTrackingActive} first.
     */
    private static Variable[] writeDynamicAllocationCheck(
        WriteScope writeScope,
        MethodWriter methodWriter,
        String role,
        Class<?>[] parameterTypes,
        java.lang.reflect.Method allocationEstimator
    ) {
        Variable[] operands = spillCallOperands(writeScope, methodWriter, role, parameterTypes);

        loadScriptPointer(writeScope, methodWriter);
        loadCallOperands(methodWriter, operands);
        methodWriter.invokeStatic(Type.getType(allocationEstimator.getDeclaringClass()), Method.getMethod(allocationEstimator));
        methodWriter.invokeStatic(WriterConstants.ALLOCATION_GUARD_TYPE, WriterConstants.SANITIZE_ESTIMATE);
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, WriterConstants.CHECK_ALLOC_BYTES);

        return operands;
    }

    /** True if the {@code #allocLimit} marker is present, meaning tracking is on and a script pointer is reachable. */
    private static boolean isAllocationTrackingActive(WriteScope writeScope) {
        return writeScope.getInternalVariable("allocLimit") != null;
    }

    /** Pushes the script instance as {@code PainlessScript}: {@code this} for instance methods, {@code #scriptThis} for static lambdas. */
    private static void loadScriptPointer(WriteScope writeScope, MethodWriter methodWriter) {
        Variable thisVariable = writeScope.getInternalVariable("this");

        if (thisVariable != null) {
            methodWriter.loadThis();
        } else {
            Variable scriptThis = writeScope.getInternalVariable("scriptThis");
            methodWriter.visitVarInsn(Opcodes.ALOAD, scriptThis.getSlot());
            methodWriter.checkCast(BASE_INTERFACE_TYPE);
        }
    }

    @Override
    public void visitField(FieldNode irFieldNode, WriteScope writeScope) {
        int access = ClassWriter.buildAccess(irFieldNode.getDecorationValue(IRDModifiers.class), true);
        String name = irFieldNode.getDecorationValue(IRDName.class);
        String descriptor = Type.getType(irFieldNode.getDecorationValue(IRDFieldType.class)).getDescriptor();

        ClassWriter classWriter = writeScope.getClassWriter();
        classWriter.getClassVisitor().visitField(access, name, descriptor, null, null).visitEnd();
    }

    @Override
    public void visitBlock(BlockNode irBlockNode, WriteScope writeScope) {
        for (StatementNode statementNode : irBlockNode.getStatementsNodes()) {
            visit(statementNode, writeScope);
        }
    }

    @Override
    public void visitIf(IfNode irIfNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irIfNode.getLocation());

        Label fals = new Label();

        visit(irIfNode.getConditionNode(), writeScope);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);
        visit(irIfNode.getBlockNode(), writeScope.newBlockScope());
        methodWriter.mark(fals);
    }

    @Override
    public void visitIfElse(IfElseNode irIfElseNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irIfElseNode.getLocation());

        Label fals = new Label();
        Label end = new Label();

        visit(irIfElseNode.getConditionNode(), writeScope);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);
        visit(irIfElseNode.getBlockNode(), writeScope.newBlockScope());

        if (irIfElseNode.getBlockNode().hasCondition(IRCAllEscape.class) == false) {
            methodWriter.goTo(end);
        }

        methodWriter.mark(fals);
        visit(irIfElseNode.getElseBlockNode(), writeScope.newBlockScope());
        methodWriter.mark(end);
    }

    @Override
    public void visitWhileLoop(WhileLoopNode irWhileLoopNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irWhileLoopNode.getLocation());

        writeScope = writeScope.newBlockScope();

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        if (irWhileLoopNode.hasCondition(IRCContinuous.class) == false) {
            visit(irWhileLoopNode.getConditionNode(), writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        writeBranchedLoopGuard(writeScope, methodWriter, irWhileLoopNode.getLocation(), true);

        BlockNode irBlockNode = irWhileLoopNode.getBlockNode();

        if (irBlockNode != null) {
            visit(irBlockNode, writeScope.newLoopScope(begin, end));
        }

        if (irBlockNode == null || irBlockNode.hasCondition(IRCAllEscape.class) == false) {
            methodWriter.goTo(begin);
        }

        methodWriter.mark(end);
    }

    @Override
    public void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irDoWhileLoopNode.getLocation());

        writeScope = writeScope.newBlockScope();

        Label start = new Label();
        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(start);
        visit(irDoWhileLoopNode.getBlockNode(), writeScope.newLoopScope(begin, end));
        methodWriter.mark(begin);

        if (irDoWhileLoopNode.hasCondition(IRCContinuous.class) == false) {
            visit(irDoWhileLoopNode.getConditionNode(), writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        writeBranchedLoopGuard(writeScope, methodWriter, irDoWhileLoopNode.getLocation(), true);

        methodWriter.goTo(start);
        methodWriter.mark(end);
    }

    @Override
    public void visitForLoop(ForLoopNode irForLoopNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irForLoopNode.getLocation());

        IRNode irInitializerNode = irForLoopNode.getInitializerNode();
        ExpressionNode irConditionNode = irForLoopNode.getConditionNode();
        ExpressionNode irAfterthoughtNode = irForLoopNode.getAfterthoughtNode();
        BlockNode irBlockNode = irForLoopNode.getBlockNode();

        writeScope = writeScope.newBlockScope();

        Label start = new Label();
        Label begin = irAfterthoughtNode == null ? start : new Label();
        Label end = new Label();

        if (irInitializerNode instanceof DeclarationBlockNode) {
            visit(irInitializerNode, writeScope);
        } else if (irInitializerNode instanceof ExpressionNode irExpressionNode) {

            visit(irExpressionNode, writeScope);
            methodWriter.writePop(MethodWriter.getType(irExpressionNode.getDecorationValue(IRDExpressionType.class)).getSize());
        }

        methodWriter.mark(start);

        if (irConditionNode != null && irForLoopNode.hasCondition(IRCContinuous.class) == false) {
            visit(irConditionNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        writeBranchedLoopGuard(writeScope, methodWriter, irForLoopNode.getLocation(), true);

        boolean allEscape = false;

        if (irBlockNode != null) {
            allEscape = irBlockNode.hasCondition(IRCAllEscape.class);
            visit(irBlockNode, writeScope.newLoopScope(begin, end));
        }

        if (irAfterthoughtNode != null) {
            methodWriter.mark(begin);
            visit(irAfterthoughtNode, writeScope);
            methodWriter.writePop(MethodWriter.getType(irAfterthoughtNode.getDecorationValue(IRDExpressionType.class)).getSize());
        }

        if (irAfterthoughtNode != null || allEscape == false) {
            methodWriter.goTo(start);
        }

        methodWriter.mark(end);
    }

    @Override
    public void visitForEachLoop(ForEachLoopNode irForEachLoopNode, WriteScope writeScope) {
        visit(irForEachLoopNode.getConditionNode(), writeScope.newBlockScope());
    }

    @Override
    public void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irForEachSubArrayNode.getLocation());

        Variable variable = writeScope.defineVariable(
            irForEachSubArrayNode.getDecorationValue(IRDVariableType.class),
            irForEachSubArrayNode.getDecorationValue(IRDVariableName.class)
        );
        Variable array = writeScope.defineInternalVariable(
            irForEachSubArrayNode.getDecorationValue(IRDArrayType.class),
            irForEachSubArrayNode.getDecorationValue(IRDArrayName.class)
        );
        Variable index = writeScope.defineInternalVariable(
            irForEachSubArrayNode.getDecorationValue(IRDIndexType.class),
            irForEachSubArrayNode.getDecorationValue(IRDIndexName.class)
        );

        visit(irForEachSubArrayNode.getConditionNode(), writeScope);
        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ISTORE), array.getSlot());
        methodWriter.push(-1);
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ISTORE), index.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitIincInsn(index.getSlot(), 1);
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.arrayLength();
        methodWriter.ifICmp(MethodWriter.GE, end);

        writeBranchedLoopGuard(writeScope, methodWriter, irForEachSubArrayNode.getLocation(), false);

        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.arrayLoad(MethodWriter.getType(irForEachSubArrayNode.getDecorationValue(IRDIndexedType.class)));
        methodWriter.writeCast(irForEachSubArrayNode.getDecorationValue(IRDCast.class));
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        visit(irForEachSubArrayNode.getBlockNode(), writeScope.newLoopScope(begin, end));

        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }

    @Override
    public void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irForEachSubIterableNode.getLocation());

        PainlessMethod painlessMethod = irForEachSubIterableNode.getDecorationValue(IRDMethod.class);

        Variable variable = writeScope.defineVariable(
            irForEachSubIterableNode.getDecorationValue(IRDVariableType.class),
            irForEachSubIterableNode.getDecorationValue(IRDVariableName.class)
        );
        Variable iterator = writeScope.defineInternalVariable(
            irForEachSubIterableNode.getDecorationValue(IRDIterableType.class),
            irForEachSubIterableNode.getDecorationValue(IRDIterableName.class)
        );

        visit(irForEachSubIterableNode.getConditionNode(), writeScope);

        if (painlessMethod == null) {
            Type methodType = Type.getMethodType(Type.getType(ValueIterator.class), Type.getType(Object.class));
            methodWriter.invokeDefCall("iterator", methodType, DefBootstrap.ITERATOR);
        } else {
            methodWriter.invokeMethodCall(painlessMethod);
        }

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ISTORE), iterator.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_HASNEXT);
        methodWriter.ifZCmp(MethodWriter.EQ, end);

        writeBranchedLoopGuard(writeScope, methodWriter, irForEachSubIterableNode.getLocation(), false);

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        if (painlessMethod != null || variable.getType().isPrimitive() == false) {
            methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_NEXT);
            methodWriter.writeCast(irForEachSubIterableNode.getDecorationValue(IRDCast.class));
        } else {
            switch (variable.getAsmType().getSort()) {
                case Type.BOOLEAN -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_BOOLEAN);
                case Type.BYTE -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_BYTE);
                case Type.SHORT -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_SHORT);
                case Type.CHAR -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_CHAR);
                case Type.INT -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_INT);
                case Type.LONG -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_LONG);
                case Type.FLOAT -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_FLOAT);
                case Type.DOUBLE -> methodWriter.invokeInterface(VALUE_ITERATOR_TYPE, VALUE_ITERATOR_NEXT_DOUBLE);
                default -> throw new IllegalArgumentException("Unknown primitive iteration variable type " + variable.getAsmType());
            }
        }
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        visit(irForEachSubIterableNode.getBlockNode(), writeScope.newLoopScope(begin, end));
        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }

    @Override
    public void visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, WriteScope writeScope) {
        for (DeclarationNode declarationNode : irDeclarationBlockNode.getDeclarationsNodes()) {
            visit(declarationNode, writeScope);
        }
    }

    @Override
    public void visitDeclaration(DeclarationNode irDeclarationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irDeclarationNode.getLocation());

        Class<?> variableType = irDeclarationNode.getDecorationValue(IRDDeclarationType.class);
        String variableName = irDeclarationNode.getDecorationValue(IRDName.class);
        Variable variable = writeScope.defineVariable(variableType, variableName);

        if (irDeclarationNode.getExpressionNode() == null) {
            Class<?> sort = variable.getType();

            if (sort == void.class
                || sort == boolean.class
                || sort == byte.class
                || sort == short.class
                || sort == char.class
                || sort == int.class) {
                methodWriter.push(0);
            } else if (sort == long.class) {
                methodWriter.push(0L);
            } else if (sort == float.class) {
                methodWriter.push(0F);
            } else if (sort == double.class) {
                methodWriter.push(0D);
            } else {
                methodWriter.visitInsn(Opcodes.ACONST_NULL);
            }
        } else {
            visit(irDeclarationNode.getExpressionNode(), writeScope);
        }

        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());
    }

    @Override
    public void visitReturn(ReturnNode irReturnNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irReturnNode.getLocation());

        if (irReturnNode.getExpressionNode() != null) {
            visit(irReturnNode.getExpressionNode(), writeScope);
        }

        methodWriter.returnValue();
    }

    @Override
    public void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irStatementExpressionNode.getLocation());
        visit(irStatementExpressionNode.getExpressionNode(), writeScope);

        Class<?> expressionType = irStatementExpressionNode.getExpressionNode().getDecorationValue(IRDExpressionType.class);
        Type asmExpressionType = MethodWriter.getType(expressionType);
        methodWriter.writePop(asmExpressionType.getSize());
    }

    @Override
    public void visitTry(TryNode irTryNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irTryNode.getLocation());

        Label tryBeginLabel = new Label();
        Label tryEndLabel = new Label();
        Label catchesEndLabel = new Label();

        methodWriter.mark(tryBeginLabel);

        visit(irTryNode.getBlockNode(), writeScope.newBlockScope());

        if (irTryNode.getBlockNode().hasCondition(IRCAllEscape.class) == false) {
            methodWriter.goTo(catchesEndLabel);
        }

        methodWriter.mark(tryEndLabel);

        List<CatchNode> catchNodes = irTryNode.getCatchNodes();

        for (int i = 0; i < catchNodes.size(); ++i) {
            CatchNode irCatchNode = catchNodes.get(i);
            boolean innerCatch = catchNodes.size() > 1 && i < catchNodes.size() - 1;
            Label catchJumpLabel = innerCatch ? catchesEndLabel : null;
            visit(irCatchNode, writeScope.newTryScope(tryBeginLabel, tryEndLabel, catchJumpLabel));
        }

        if (irTryNode.getBlockNode().hasCondition(IRCAllEscape.class) == false || catchNodes.size() > 1) {
            methodWriter.mark(catchesEndLabel);
        }
    }

    @Override
    public void visitCatch(CatchNode irCatchNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irCatchNode.getLocation());

        Class<?> exceptionType = irCatchNode.getDecorationValue(IRDExceptionType.class);
        String exceptionName = irCatchNode.getDecorationValue(IRDSymbol.class);
        Variable variable = writeScope.defineVariable(exceptionType, exceptionName);

        Label jump = new Label();

        methodWriter.mark(jump);
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        BlockNode irBlockNode = irCatchNode.getBlockNode();

        if (irBlockNode != null) {
            visit(irBlockNode, writeScope.newBlockScope(true));
        }

        methodWriter.visitTryCatchBlock(
            writeScope.getTryBeginLabel(),
            writeScope.getTryEndLabel(),
            jump,
            variable.getAsmType().getInternalName()
        );

        if (writeScope.getCatchesEndLabel() != null && (irBlockNode == null || irBlockNode.hasCondition(IRCAllEscape.class) == false)) {
            methodWriter.goTo(writeScope.getCatchesEndLabel());
        }
    }

    @Override
    public void visitThrow(ThrowNode irThrowNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irThrowNode.getLocation());
        visit(irThrowNode.getExpressionNode(), writeScope);
        methodWriter.throwException();
    }

    @Override
    public void visitContinue(ContinueNode irContinueNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.goTo(writeScope.getContinueLabel());
    }

    @Override
    public void visitBreak(BreakNode irBreakNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.goTo(writeScope.getBreakLabel());
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, WriteScope writeScope) {
        visit(irBinaryImplNode.getLeftNode(), writeScope);
        visit(irBinaryImplNode.getRightNode(), writeScope);
    }

    @Override
    public void visitUnaryMath(UnaryMathNode irUnaryMathNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irUnaryMathNode.getLocation());

        Operation operation = irUnaryMathNode.getDecorationValue(IRDOperation.class);

        if (operation == Operation.NOT) {
            Label fals = new Label();
            Label end = new Label();

            visit(irUnaryMathNode.getChildNode(), writeScope);

            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(true);
            methodWriter.mark(end);
        } else {
            visit(irUnaryMathNode.getChildNode(), writeScope);

            Type actualType = MethodWriter.getType(irUnaryMathNode.getDecorationValue(IRDExpressionType.class));
            Type childType = MethodWriter.getType(irUnaryMathNode.getChildNode().getDecorationValue(IRDExpressionType.class));

            Class<?> unaryType = irUnaryMathNode.getDecorationValue(IRDUnaryType.class);
            int flags = irUnaryMathNode.getDecorationValueOrDefault(IRDFlags.class, 0);

            if (operation == Operation.BWNOT) {
                if (unaryType == def.class) {
                    Type descriptor = Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("not", descriptor, DefBootstrap.UNARY_OPERATOR, flags);
                } else {
                    if (unaryType == int.class) {
                        methodWriter.push(-1);
                    } else if (unaryType == long.class) {
                        methodWriter.push(-1L);
                    } else {
                        throw new IllegalStateException(
                            Strings.format(
                                "unexpected unary math operation [%s] for type [%s]",
                                operation,
                                irUnaryMathNode.getDecorationString(IRDExpressionType.class)
                            )
                        );
                    }

                    methodWriter.math(MethodWriter.XOR, actualType);
                }
            } else if (operation == Operation.SUB) {
                if (unaryType == def.class) {
                    Type descriptor = Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("neg", descriptor, DefBootstrap.UNARY_OPERATOR, flags);
                } else {
                    methodWriter.math(MethodWriter.NEG, actualType);
                }
            } else if (operation == Operation.ADD) {
                if (unaryType == def.class) {
                    Type descriptor = Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("plus", descriptor, DefBootstrap.UNARY_OPERATOR, flags);
                }
            } else {
                throw new IllegalStateException(
                    Strings.format(
                        "unexpected unary math operation [%s] for type [%s]",
                        operation,
                        irUnaryMathNode.getDecorationString(IRDExpressionType.class)
                    )
                );
            }
        }
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irBinaryMathNode.getLocation());

        Operation operation = irBinaryMathNode.getDecorationValue(IRDOperation.class);
        ExpressionNode irLeftNode = irBinaryMathNode.getLeftNode();
        ExpressionNode irRightNode = irBinaryMathNode.getRightNode();

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            visit(irRightNode, writeScope);
            methodWriter.push(irBinaryMathNode.getDecorationValue(IRDRegexLimit.class));
            visit(irLeftNode, writeScope);
            methodWriter.invokeStatic(Type.getType(Augmentation.class), WriterConstants.PATTERN_MATCHER);

            if (operation == Operation.FIND) {
                methodWriter.invokeVirtual(Type.getType(Matcher.class), WriterConstants.MATCHER_FIND);
            } else if (operation == Operation.MATCH) {
                methodWriter.invokeVirtual(Type.getType(Matcher.class), WriterConstants.MATCHER_MATCHES);
            } else {
                throw new IllegalStateException(
                    Strings.format(
                        "unexpected binary math operation [%s] for type [%s]",
                        operation,
                        irBinaryMathNode.getDecorationString(IRDExpressionType.class)
                    )
                );
            }
        } else {
            Class<?> expressionType = irBinaryMathNode.getDecorationValue(IRDExpressionType.class);
            Class<?> leftType = irLeftNode.getDecorationValue(IRDExpressionType.class);
            Class<?> rightType = irRightNode.getDecorationValue(IRDExpressionType.class);
            boolean dynamic = irBinaryMathNode.getDecorationValue(IRDBinaryType.class) == def.class
                || (irBinaryMathNode.getDecoration(IRDShiftType.class) != null
                    && irBinaryMathNode.getDecorationValue(IRDShiftType.class) == def.class);
            int flags = irBinaryMathNode.getDecorationValueOrDefault(IRDFlags.class, 0);

            // A def '+' may be a runtime string concat: spill operands, charge (checkDefConcatAlloc charges only if an operand
            // is a String), reload, then the real add. Other ops keep the zero-overhead emission.
            if (dynamic && operation == Operation.ADD && isAllocationTrackingActive(writeScope)) {
                visit(irLeftNode, writeScope);
                Variable left = writeScope.defineInternalVariable(leftType, "defConcatLeft");
                methodWriter.visitVarInsn(left.getAsmType().getOpcode(Opcodes.ISTORE), left.getSlot());
                visit(irRightNode, writeScope);
                Variable right = writeScope.defineInternalVariable(rightType, "defConcatRight");
                methodWriter.visitVarInsn(right.getAsmType().getOpcode(Opcodes.ISTORE), right.getSlot());

                loadScriptPointer(writeScope, methodWriter);
                methodWriter.visitVarInsn(left.getAsmType().getOpcode(Opcodes.ILOAD), left.getSlot());
                if (leftType.isPrimitive()) {
                    methodWriter.box(MethodWriter.getType(leftType));
                }
                methodWriter.visitVarInsn(right.getAsmType().getOpcode(Opcodes.ILOAD), right.getSlot());
                if (rightType.isPrimitive()) {
                    methodWriter.box(MethodWriter.getType(rightType));
                }
                methodWriter.invokeStatic(WriterConstants.ALLOCATION_GUARD_TYPE, WriterConstants.CHECK_DEF_CONCAT_ALLOC);

                methodWriter.visitVarInsn(left.getAsmType().getOpcode(Opcodes.ILOAD), left.getSlot());
                methodWriter.visitVarInsn(right.getAsmType().getOpcode(Opcodes.ILOAD), right.getSlot());
                methodWriter.writeDynamicBinaryInstruction(
                    irBinaryMathNode.getLocation(),
                    expressionType,
                    leftType,
                    rightType,
                    operation,
                    flags
                );
                return;
            }

            visit(irLeftNode, writeScope);
            visit(irRightNode, writeScope);

            if (dynamic) {
                methodWriter.writeDynamicBinaryInstruction(
                    irBinaryMathNode.getLocation(),
                    expressionType,
                    leftType,
                    rightType,
                    operation,
                    flags
                );
            } else {
                methodWriter.writeBinaryInstruction(irBinaryMathNode.getLocation(), expressionType, operation);
            }
        }
    }

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irStringConcatenationNode.getLocation());

        List<ExpressionNode> irArgumentNodes = irStringConcatenationNode.getArgumentNodes();

        if (isAllocationTrackingActive(writeScope) == false) {
            // Tracking off: unchanged zero-overhead emission.
            methodWriter.writeNewStrings();
            for (ExpressionNode argumentNode : irArgumentNodes) {
                visit(argumentNode, writeScope);
                methodWriter.writeAppendStrings(argumentNode.getDecorationValue(IRDExpressionType.class));
            }
            methodWriter.writeToStrings();
            return;
        }

        // Tracking on: charge STRING_CONCAT_RESULT_OVERHEAD plus a per-operand byte bound before the concat allocates its
        // result. The bound is the operand's real String length at runtime (via stringConcatOperandBytes) for references and
        // a compile-time max-length constant for primitives. Operands are spilled to fresh locals first so they can be reloaded
        // for the actual concat after the check.
        int argCount = irArgumentNodes.size();
        Class<?>[] argTypes = new Class<?>[argCount];
        Variable[] concatOperands = new Variable[argCount];
        for (int i = 0; i < argCount; ++i) {
            ExpressionNode argumentNode = irArgumentNodes.get(i);
            argTypes[i] = argumentNode.getDecorationValue(IRDExpressionType.class);
            visit(argumentNode, writeScope);
            concatOperands[i] = writeScope.defineInternalVariable(argTypes[i], "concatOperand" + i);
            methodWriter.visitVarInsn(concatOperands[i].getAsmType().getOpcode(Opcodes.ISTORE), concatOperands[i].getSlot());
        }

        loadScriptPointer(writeScope, methodWriter);
        methodWriter.push((long) AllocSizes.STRING_CONCAT_RESULT_OVERHEAD);
        for (int i = 0; i < argCount; ++i) {
            if (argTypes[i].isPrimitive()) {
                methodWriter.push(AllocSizes.stringConcatPrimitiveBytes(argTypes[i]));
            } else {
                methodWriter.visitVarInsn(concatOperands[i].getAsmType().getOpcode(Opcodes.ILOAD), concatOperands[i].getSlot());
                methodWriter.invokeStatic(WriterConstants.ALLOC_SIZES_TYPE, WriterConstants.ALLOC_STRING_CONCAT_OPERAND_BYTES);
            }
            methodWriter.math(MethodWriter.ADD, Type.LONG_TYPE);
        }
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, WriterConstants.CHECK_ALLOC_BYTES);

        // Reload the spilled operands and perform the actual concat.
        methodWriter.writeNewStrings();
        for (int i = 0; i < argCount; ++i) {
            methodWriter.visitVarInsn(concatOperands[i].getAsmType().getOpcode(Opcodes.ILOAD), concatOperands[i].getSlot());
            methodWriter.writeAppendStrings(argTypes[i]);
        }
        methodWriter.writeToStrings();
    }

    @Override
    public void visitBoolean(BooleanNode irBooleanNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irBooleanNode.getLocation());

        Operation operation = irBooleanNode.getDecorationValue(IRDOperation.class);
        ExpressionNode irLeftNode = irBooleanNode.getLeftNode();
        ExpressionNode irRightNode = irBooleanNode.getRightNode();

        if (operation == Operation.AND) {
            Label fals = new Label();
            Label end = new Label();

            visit(irLeftNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);
            visit(irRightNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(true);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(false);
            methodWriter.mark(end);
        } else if (operation == Operation.OR) {
            Label tru = new Label();
            Label fals = new Label();
            Label end = new Label();

            visit(irLeftNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFNE, tru);
            visit(irRightNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.mark(tru);
            methodWriter.push(true);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(false);
            methodWriter.mark(end);
        } else {
            throw new IllegalStateException(
                "unexpected boolean operation ["
                    + operation
                    + "] "
                    + "for type ["
                    + irBooleanNode.getDecorationString(IRDExpressionType.class)
                    + "]"
            );
        }
    }

    @Override
    public void visitComparison(ComparisonNode irComparisonNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irComparisonNode.getLocation());

        Operation operation = irComparisonNode.getDecorationValue(IRDOperation.class);
        ExpressionNode irLeftNode = irComparisonNode.getLeftNode();
        ExpressionNode irRightNode = irComparisonNode.getRightNode();

        visit(irLeftNode, writeScope);

        if (irRightNode instanceof NullNode == false) {
            visit(irRightNode, writeScope);
        }

        Label jump = new Label();
        Label end = new Label();

        boolean eq = (operation == Operation.EQ || operation == Operation.EQR);
        boolean ne = (operation == Operation.NE || operation == Operation.NER);
        boolean lt = operation == Operation.LT;
        boolean lte = operation == Operation.LTE;
        boolean gt = operation == Operation.GT;
        boolean gte = operation == Operation.GTE;

        boolean writejump = true;

        Class<?> comparisonType = irComparisonNode.getDecorationValue(IRDComparisonType.class);
        Type type = MethodWriter.getType(comparisonType);

        if (comparisonType == void.class || comparisonType == byte.class || comparisonType == short.class || comparisonType == char.class) {
            throw new IllegalStateException(
                Strings.format(
                    "unexpected comparison operation [%s] for type [%s]",
                    operation,
                    irComparisonNode.getDecorationString(IRDExpressionType.class)
                )
            );
        } else if (comparisonType == boolean.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else {
                throw new IllegalStateException(
                    Strings.format(
                        "unexpected comparison operation [%s] for type [%s]",
                        operation,
                        irComparisonNode.getDecorationString(IRDExpressionType.class)
                    )
                );
            }
        } else if (comparisonType == int.class
            || comparisonType == long.class
            || comparisonType == float.class
            || comparisonType == double.class) {
                if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
                else if (lt) methodWriter.ifCmp(type, MethodWriter.LT, jump);
                else if (lte) methodWriter.ifCmp(type, MethodWriter.LE, jump);
                else if (gt) methodWriter.ifCmp(type, MethodWriter.GT, jump);
                else if (gte) methodWriter.ifCmp(type, MethodWriter.GE, jump);
                else {
                    throw new IllegalStateException(
                        Strings.format(
                            "unexpected comparison operation [%s] for type [%s]",
                            operation,
                            irComparisonNode.getDecorationString(IRDExpressionType.class)
                        )
                    );
                }

            } else if (comparisonType == def.class) {
                Type booleanType = Type.getType(boolean.class);
                Type descriptor = Type.getMethodType(
                    booleanType,
                    MethodWriter.getType(irLeftNode.getDecorationValue(IRDExpressionType.class)),
                    MethodWriter.getType(irRightNode.getDecorationValue(IRDExpressionType.class))
                );

                if (eq) {
                    if (irRightNode instanceof NullNode) {
                        methodWriter.ifNull(jump);
                    } else if (irLeftNode instanceof NullNode == false && operation == Operation.EQ) {
                        methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                        writejump = false;
                    } else {
                        methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                    }
                } else if (ne) {
                    if (irRightNode instanceof NullNode) {
                        methodWriter.ifNonNull(jump);
                    } else if (irLeftNode instanceof NullNode == false && operation == Operation.NE) {
                        methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                        methodWriter.ifZCmp(MethodWriter.EQ, jump);
                    } else {
                        methodWriter.ifCmp(type, MethodWriter.NE, jump);
                    }
                } else if (lt) {
                    methodWriter.invokeDefCall("lt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                    writejump = false;
                } else if (lte) {
                    methodWriter.invokeDefCall("lte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                    writejump = false;
                } else if (gt) {
                    methodWriter.invokeDefCall("gt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                    writejump = false;
                } else if (gte) {
                    methodWriter.invokeDefCall("gte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                    writejump = false;
                } else {
                    throw new IllegalStateException(
                        Strings.format(
                            "unexpected comparison operation [%s] for type [%s]",
                            operation,
                            irComparisonNode.getDecorationString(IRDExpressionType.class)
                        )
                    );
                }
            } else {
                if (eq) {
                    if (irRightNode instanceof NullNode) {
                        methodWriter.ifNull(jump);
                    } else if (operation == Operation.EQ) {
                        methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                        writejump = false;
                    } else {
                        methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                    }
                } else if (ne) {
                    if (irRightNode instanceof NullNode) {
                        methodWriter.ifNonNull(jump);
                    } else if (operation == Operation.NE) {
                        methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                        methodWriter.ifZCmp(MethodWriter.EQ, jump);
                    } else {
                        methodWriter.ifCmp(type, MethodWriter.NE, jump);
                    }
                } else {
                    throw new IllegalStateException(
                        Strings.format(
                            "unexpected comparison operation [%s] for type [%s]",
                            operation,
                            irComparisonNode.getDecorationString(IRDExpressionType.class)
                        )
                    );
                }
            }

        if (writejump) {
            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(jump);
            methodWriter.push(true);
            methodWriter.mark(end);
        }
    }

    @Override
    public void visitCast(CastNode irCastNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irCastNode.getChildNode(), writeScope);
        methodWriter.writeDebugInfo(irCastNode.getLocation());

        // A cast that boxes a primitive allocates a wrapper. The value to box is already on the stack; the pre-check has a
        // net-zero stack effect, so emit it before writeCast performs the box.
        PainlessCast cast = irCastNode.getDecorationValue(IRDCast.class);
        // boxTargetType / boxOriginalType hold the primitive being boxed when the cast allocates a wrapper.
        Class<?> boxType = cast.boxTargetType != null ? cast.boxTargetType : cast.boxOriginalType;
        if (boxType != null) {
            writeAllocationCheck(writeScope, AllocSizes.boxSize(boxType));
        }

        methodWriter.writeCast(cast);
    }

    @Override
    public void visitInstanceof(InstanceofNode irInstanceofNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        ExpressionNode irChildNode = irInstanceofNode.getChildNode();

        visit(irChildNode, writeScope);

        Class<?> instanceType = irInstanceofNode.getDecorationValue(IRDInstanceType.class);
        Class<?> expressionType = irInstanceofNode.getDecorationValue(IRDExpressionType.class);

        if (instanceType == def.class) {
            methodWriter.writePop(MethodWriter.getType(expressionType).getSize());
            methodWriter.push(true);
        } else if (irChildNode.getDecorationValue(IRDExpressionType.class).isPrimitive()) {
            Class<?> boxedInstanceType = PainlessLookupUtility.typeToBoxedType(instanceType);
            Class<?> childExpressionType = irChildNode.getDecorationValue(IRDExpressionType.class);
            Class<?> boxedExpressionType = PainlessLookupUtility.typeToBoxedType(childExpressionType);

            methodWriter.writePop(MethodWriter.getType(expressionType).getSize());
            methodWriter.push(boxedInstanceType.isAssignableFrom(boxedExpressionType));
        } else {
            methodWriter.instanceOf(MethodWriter.getType(PainlessLookupUtility.typeToBoxedType(instanceType)));
        }
    }

    @Override
    public void visitConditional(ConditionalNode irConditionalNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irConditionalNode.getLocation());

        Label fals = new Label();
        Label end = new Label();

        visit(irConditionalNode.getConditionNode(), writeScope);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);

        visit(irConditionalNode.getLeftNode(), writeScope);
        methodWriter.goTo(end);
        methodWriter.mark(fals);
        visit(irConditionalNode.getRightNode(), writeScope);
        methodWriter.mark(end);
    }

    @Override
    public void visitElvis(ElvisNode irElvisNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irElvisNode.getLocation());

        Label end = new Label();

        visit(irElvisNode.getLeftNode(), writeScope);
        methodWriter.dup();
        methodWriter.ifNonNull(end);
        methodWriter.pop();
        visit(irElvisNode.getRightNode(), writeScope);
        methodWriter.mark(end);
    }

    @Override
    public void visitListInitialization(ListInitializationNode irListInitializationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irListInitializationNode.getLocation());

        PainlessConstructor painlessConstructor = irListInitializationNode.getDecorationValue(IRDConstructor.class);
        methodWriter.newInstance(MethodWriter.getType(irListInitializationNode.getDecorationValue(IRDExpressionType.class)));
        methodWriter.dup();
        methodWriter.invokeConstructor(
            Type.getType(painlessConstructor.javaConstructor().getDeclaringClass()),
            Method.getMethod(painlessConstructor.javaConstructor())
        );

        for (ExpressionNode irArgumentNode : irListInitializationNode.getArgumentNodes()) {
            methodWriter.dup();
            visit(irArgumentNode, writeScope);
            methodWriter.invokeMethodCall(irListInitializationNode.getDecorationValue(IRDMethod.class));
            methodWriter.pop();
        }
    }

    @Override
    public void visitMapInitialization(MapInitializationNode irMapInitializationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irMapInitializationNode.getLocation());

        PainlessConstructor painlessConstructor = irMapInitializationNode.getDecorationValue(IRDConstructor.class);
        methodWriter.newInstance(MethodWriter.getType(irMapInitializationNode.getDecorationValue(IRDExpressionType.class)));
        methodWriter.dup();
        methodWriter.invokeConstructor(
            Type.getType(painlessConstructor.javaConstructor().getDeclaringClass()),
            Method.getMethod(painlessConstructor.javaConstructor())
        );

        for (int index = 0; index < irMapInitializationNode.getArgumentsSize(); ++index) {
            methodWriter.dup();
            visit(irMapInitializationNode.getKeyNode(index), writeScope);
            visit(irMapInitializationNode.getValueNode(index), writeScope);
            methodWriter.invokeMethodCall(irMapInitializationNode.getDecorationValue(IRDMethod.class));
            methodWriter.pop();
        }
    }

    @Override
    public void visitNewArray(NewArrayNode irNewArrayNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNewArrayNode.getLocation());

        List<ExpressionNode> irArgumentNodes = irNewArrayNode.getArgumentNodes();
        Class<?> expressionType = irNewArrayNode.getDecorationValue(IRDExpressionType.class);

        if (irNewArrayNode.hasCondition(IRCInitialize.class)) {
            // new T[]{ ... }: the element count is the number of initializers, known at compile time.
            Class<?> componentType = expressionType.getComponentType();
            int count = irArgumentNodes.size();
            writeAllocationCheck(writeScope, AllocSizes.arraySize(componentType, count));

            methodWriter.push(count);
            methodWriter.newArray(MethodWriter.getType(componentType));

            for (int index = 0; index < irArgumentNodes.size(); ++index) {
                ExpressionNode irArgumentNode = irArgumentNodes.get(index);

                methodWriter.dup();
                methodWriter.push(index);
                visit(irArgumentNode, writeScope);
                methodWriter.arrayStore(MethodWriter.getType(expressionType.getComponentType()));
            }
        } else if (isAllocationTrackingActive(writeScope) == false) {
            // Tracking off: no allocation-tracking overhead.
            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            if (irArgumentNodes.size() > 1) {
                methodWriter.visitMultiANewArrayInsn(MethodWriter.getType(expressionType).getDescriptor(), irArgumentNodes.size());
            } else {
                methodWriter.newArray(MethodWriter.getType(expressionType.getComponentType()));
            }
        } else {
            // Tracking on: charge AllocSizes.arrayBytes(product(dims), fieldSize(innermostType)). The product is folded with
            // a saturating multiply so an overflowing multi-dim extent yields Long.MAX_VALUE and trips the limit rather than
            // wrapping to a small, under-counted charge. Spill dims to locals first so they can be reloaded for the
            // allocation instruction after the check.
            int dimCount = irArgumentNodes.size();
            Class<?> innermostComponentType = expressionType;
            for (int k = 0; k < dimCount; ++k) {
                innermostComponentType = innermostComponentType.getComponentType();
            }
            int[] dimSlots = new int[dimCount];
            for (int k = 0; k < dimCount; ++k) {
                visit(irArgumentNodes.get(k), writeScope);
                dimSlots[k] = writeScope.defineInternalVariable(int.class, "arrayDim" + k).getSlot();
            }
            // Stack after visits: [d0, ..., dN-1] with dN-1 on top; spill in reverse.
            for (int k = dimCount - 1; k >= 0; --k) {
                methodWriter.visitVarInsn(Opcodes.ISTORE, dimSlots[k]);
            }
            loadScriptPointer(writeScope, methodWriter);
            methodWriter.visitVarInsn(Opcodes.ILOAD, dimSlots[0]);
            methodWriter.visitInsn(Opcodes.I2L);
            for (int k = 1; k < dimCount; ++k) {
                methodWriter.visitVarInsn(Opcodes.ILOAD, dimSlots[k]);
                methodWriter.visitInsn(Opcodes.I2L);
                methodWriter.invokeStatic(WriterConstants.ALLOC_SIZES_TYPE, WriterConstants.ALLOC_MUL_SAT);
            }
            methodWriter.push(AllocSizes.fieldSize(innermostComponentType));
            methodWriter.invokeStatic(WriterConstants.ALLOC_SIZES_TYPE, WriterConstants.ALLOC_ARRAY_BYTES);
            methodWriter.invokeInterface(BASE_INTERFACE_TYPE, WriterConstants.CHECK_ALLOC_BYTES);
            for (int k = 0; k < dimCount; ++k) {
                methodWriter.visitVarInsn(Opcodes.ILOAD, dimSlots[k]);
            }
            if (dimCount == 1) {
                methodWriter.newArray(MethodWriter.getType(innermostComponentType));
            } else {
                methodWriter.visitMultiANewArrayInsn(MethodWriter.getType(expressionType).getDescriptor(), dimCount);
            }
        }
    }

    @Override
    public void visitNewObject(NewObjectNode irNewObjectNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNewObjectNode.getLocation());

        PainlessConstructor painlessConstructor = irNewObjectNode.getDecorationValue(IRDConstructor.class);

        // Sizing new T() needs the class's field layout, which is the allowlist's domain, so the construction cost is carried as
        // an @allocates estimator on the constructor and the charge lands before the object is allocated.
        java.lang.reflect.Method constructorEstimator = irNewObjectNode.getDecorationValue(IRDAllocationEstimator.class);
        if (constructorEstimator != null && isAllocationTrackingActive(writeScope)) {
            // Standard emission is NEW + DUP + <args> + INVOKESPECIAL, but the estimator needs the argument values and the
            // charge must land before NEW allocates. Reorder: evaluate args in source order, spill, estimate + charge, then
            // allocate and replay the args.
            for (ExpressionNode irArgumentNode : irNewObjectNode.getArgumentNodes()) {
                visit(irArgumentNode, writeScope);
            }

            Variable[] operands = writeDynamicAllocationCheck(
                writeScope,
                methodWriter,
                "newObjectArg",
                painlessConstructor.methodType().parameterArray(),
                constructorEstimator
            );

            methodWriter.newInstance(MethodWriter.getType(irNewObjectNode.getDecorationValue(IRDExpressionType.class)));
            methodWriter.dup();
            loadCallOperands(methodWriter, operands);
        } else {
            methodWriter.newInstance(MethodWriter.getType(irNewObjectNode.getDecorationValue(IRDExpressionType.class)));

            // Always dup so that visitStatementExpression's always has something to pop
            methodWriter.dup();

            for (ExpressionNode irArgumentNode : irNewObjectNode.getArgumentNodes()) {
                visit(irArgumentNode, writeScope);
            }
        }

        methodWriter.invokeConstructor(
            Type.getType(painlessConstructor.javaConstructor().getDeclaringClass()),
            Method.getMethod(painlessConstructor.javaConstructor())
        );
    }

    @Override
    public void visitConstant(ConstantNode irConstantNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        Object constant = irConstantNode.getDecorationValue(IRDConstant.class);

        if (constant instanceof String) methodWriter.push((String) constant);
        else if (constant instanceof Double) methodWriter.push((double) constant);
        else if (constant instanceof Float) methodWriter.push((float) constant);
        else if (constant instanceof Long) methodWriter.push((long) constant);
        else if (constant instanceof Integer) methodWriter.push((int) constant);
        else if (constant instanceof Character) methodWriter.push((char) constant);
        else if (constant instanceof Short) methodWriter.push((short) constant);
        else if (constant instanceof Byte) methodWriter.push((byte) constant);
        else if (constant instanceof Boolean) methodWriter.push((boolean) constant);
        else {
            /*
             * The constant doesn't properly fit into the constant pool so
             * we should have made a static field for it.
             */
            String fieldName = irConstantNode.getDecorationValue(IRDConstantFieldName.class);
            Type asmFieldType = MethodWriter.getType(irConstantNode.getDecorationValue(IRDExpressionType.class));
            if (asmFieldType == null) {
                throw irConstantNode.getLocation()
                    .createError(new IllegalStateException("Didn't attach constant to [" + irConstantNode + "]"));
            }
            methodWriter.getStatic(CLASS_TYPE, fieldName, asmFieldType);
        }
    }

    @Override
    public void visitNull(NullNode irNullNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.visitInsn(Opcodes.ACONST_NULL);
    }

    @Override
    public void visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irDefInterfaceReferenceNode.getLocation());

        // place holder for functional interface receiver
        // which is resolved and replace at runtime
        methodWriter.push((String) null);

        if (irDefInterfaceReferenceNode.hasCondition(IRCInstanceCapture.class)) {
            Variable capturedThis = writeScope.getInternalVariable("this");
            methodWriter.visitVarInsn(CLASS_TYPE.getOpcode(Opcodes.ILOAD), capturedThis.getSlot());
        }

        List<String> captureNames = irDefInterfaceReferenceNode.getDecorationValue(IRDCaptureNames.class);
        boolean captureBox = irDefInterfaceReferenceNode.hasCondition(IRCCaptureBox.class);

        if (captureNames != null) {
            for (String captureName : captureNames) {
                Variable captureVariable = writeScope.getVariable(captureName);
                methodWriter.visitVarInsn(captureVariable.getAsmType().getOpcode(Opcodes.ILOAD), captureVariable.getSlot());

                if (captureBox) {
                    methodWriter.box(captureVariable.getAsmType());
                    captureBox = false;
                }
            }
        }
    }

    @Override
    public void visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irTypedInterfaceReferenceNode.getLocation());

        List<String> captureNames = irTypedInterfaceReferenceNode.getDecorationValue(IRDCaptureNames.class);
        boolean captureBox = irTypedInterfaceReferenceNode.hasCondition(IRCCaptureBox.class);

        // Building the lambda instance allocates a capture object with one slot per captured value. The capture count
        // includes the implicit `this` capture and the synthetic #scriptThis capture (already present in captureNames when
        // injected). Charge it before any captures are loaded; the pre-check has a net-zero stack effect.
        int captureCount = (irTypedInterfaceReferenceNode.hasCondition(IRCInstanceCapture.class) ? 1 : 0) + (captureNames == null
            ? 0
            : captureNames.size());
        writeAllocationCheck(writeScope, AllocSizes.captureSize(captureCount));

        if (irTypedInterfaceReferenceNode.hasCondition(IRCInstanceCapture.class)) {
            Variable capturedThis = writeScope.getInternalVariable("this");
            methodWriter.visitVarInsn(CLASS_TYPE.getOpcode(Opcodes.ILOAD), capturedThis.getSlot());
        }

        if (captureNames != null) {
            for (String captureName : captureNames) {
                Variable captureVariable = writeScope.getVariable(captureName);
                methodWriter.visitVarInsn(captureVariable.getAsmType().getOpcode(Opcodes.ILOAD), captureVariable.getSlot());

                if (captureBox) {
                    methodWriter.box(captureVariable.getAsmType());
                    captureBox = false;
                }
            }
        }

        methodWriter.invokeLambdaCall(irTypedInterfaceReferenceNode.getDecorationValue(IRDReference.class));
    }

    @Override
    public void visitTypedCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irTypedCaptureReferenceNode.getLocation());

        String methodName = irTypedCaptureReferenceNode.getDecorationValue(IRDName.class);
        Variable captured = writeScope.getVariable(irTypedCaptureReferenceNode.getDecorationValue(IRDCaptureNames.class).get(0));
        Class<?> expressionType = irTypedCaptureReferenceNode.getDecorationValue(IRDExpressionType.class);
        String expressionCanonicalTypeName = irTypedCaptureReferenceNode.getDecorationString(IRDExpressionType.class);

        methodWriter.visitVarInsn(captured.getAsmType().getOpcode(Opcodes.ILOAD), captured.getSlot());

        if (irTypedCaptureReferenceNode.hasCondition(IRCCaptureBox.class)) {
            methodWriter.box(captured.getAsmType());
        }

        Type methodType = Type.getMethodType(MethodWriter.getType(expressionType), captured.getAsmType());
        methodWriter.invokeDefCall(methodName, methodType, DefBootstrap.REFERENCE, expressionCanonicalTypeName);
    }

    @Override
    public void visitStatic(StaticNode irStaticNode, WriteScope writeScope) {
        // do nothing
    }

    @Override
    public void visitLoadVariable(LoadVariableNode irLoadVariableNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        Variable variable = writeScope.getVariable(irLoadVariableNode.getDecorationValue(IRDName.class));
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ILOAD), variable.getSlot());
    }

    @Override
    public void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNullSafeSubNode.getLocation());

        Label end = new Label();
        methodWriter.dup();
        methodWriter.ifNull(end);
        visit(irNullSafeSubNode.getChildNode(), writeScope);
        methodWriter.mark(end);
    }

    @Override
    public void visitLoadDotArrayLengthNode(LoadDotArrayLengthNode irLoadDotArrayLengthNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotArrayLengthNode.getLocation());
        methodWriter.arrayLength();
    }

    @Override
    public void visitLoadDotDef(LoadDotDefNode irLoadDotDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotDefNode.getLocation());
        Type methodType = Type.getMethodType(
            MethodWriter.getType(irLoadDotDefNode.getDecorationValue(IRDExpressionType.class)),
            MethodWriter.getType(def.class)
        );
        methodWriter.invokeDefCall(irLoadDotDefNode.getDecorationValue(IRDValue.class), methodType, DefBootstrap.LOAD);
    }

    @Override
    public void visitLoadDot(LoadDotNode irLoadDotNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotNode.getLocation());

        PainlessField painlessField = irLoadDotNode.getDecorationValue(IRDField.class);
        boolean isStatic = Modifier.isStatic(painlessField.javaField().getModifiers());
        Type asmOwnerType = Type.getType(painlessField.javaField().getDeclaringClass());
        String fieldName = painlessField.javaField().getName();
        Type asmFieldType = MethodWriter.getType(painlessField.typeParameter());

        if (isStatic) {
            methodWriter.getStatic(asmOwnerType, fieldName, asmFieldType);
        } else {
            methodWriter.getField(asmOwnerType, fieldName, asmFieldType);
        }
    }

    @Override
    public void visitLoadDotShortcut(LoadDotShortcutNode irDotSubShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irDotSubShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irDotSubShortcutNode.getDecorationValue(IRDMethod.class);
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType() != getterPainlessMethod.javaMethod().getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType()));
        }
    }

    @Override
    public void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadListShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irLoadListShortcutNode.getDecorationValue(IRDMethod.class);
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType() != getterPainlessMethod.javaMethod().getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType()));
        }
    }

    @Override
    public void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadMapShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irLoadMapShortcutNode.getDecorationValue(IRDMethod.class);
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType() != getterPainlessMethod.javaMethod().getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType()));
        }
    }

    @Override
    public void visitLoadFieldMember(LoadFieldMemberNode irLoadFieldMemberNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadFieldMemberNode.getLocation());

        boolean isStatic = irLoadFieldMemberNode.hasCondition(IRCStatic.class);
        String memberFieldName = irLoadFieldMemberNode.getDecorationValue(IRDName.class);
        Type asmMemberFieldType = MethodWriter.getType(irLoadFieldMemberNode.getDecorationValue(IRDExpressionType.class));

        if (isStatic) {
            methodWriter.getStatic(CLASS_TYPE, memberFieldName, asmMemberFieldType);
        } else {
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, memberFieldName, asmMemberFieldType);
        }
    }

    @Override
    public void visitLoadBraceDef(LoadBraceDefNode irLoadBraceDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadBraceDefNode.getLocation());
        Type methodType = Type.getMethodType(
            MethodWriter.getType(irLoadBraceDefNode.getDecorationValue(IRDExpressionType.class)),
            MethodWriter.getType(def.class),
            MethodWriter.getType(irLoadBraceDefNode.getDecorationValue(IRDIndexType.class))
        );
        methodWriter.invokeDefCall("arrayLoad", methodType, DefBootstrap.ARRAY_LOAD);
    }

    @Override
    public void visitLoadBrace(LoadBraceNode irLoadBraceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadBraceNode.getLocation());
        methodWriter.arrayLoad(MethodWriter.getType(irLoadBraceNode.getDecorationValue(IRDExpressionType.class)));
    }

    @Override
    public void visitStoreVariable(StoreVariableNode irStoreVariableNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreVariableNode.getChildNode(), writeScope);

        Variable variable = writeScope.getVariable(irStoreVariableNode.getDecorationValue(IRDName.class));
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());
    }

    @Override
    public void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreDotDefNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreDotDefNode.getLocation());
        Type methodType = Type.getMethodType(
            MethodWriter.getType(void.class),
            MethodWriter.getType(def.class),
            MethodWriter.getType(irStoreDotDefNode.getDecorationValue(IRDStoreType.class))
        );
        methodWriter.invokeDefCall(irStoreDotDefNode.getDecorationValue(IRDValue.class), methodType, DefBootstrap.STORE);
    }

    @Override
    public void visitStoreDot(StoreDotNode irStoreDotNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreDotNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreDotNode.getLocation());

        PainlessField painlessField = irStoreDotNode.getDecorationValue(IRDField.class);
        boolean isStatic = Modifier.isStatic(painlessField.javaField().getModifiers());
        Type asmOwnerType = Type.getType(painlessField.javaField().getDeclaringClass());
        String fieldName = painlessField.javaField().getName();
        Type asmFieldType = MethodWriter.getType(painlessField.typeParameter());

        if (isStatic) {
            methodWriter.putStatic(asmOwnerType, fieldName, asmFieldType);
        } else {
            methodWriter.putField(asmOwnerType, fieldName, asmFieldType);
        }
    }

    @Override
    public void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irDotSubShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irDotSubShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irDotSubShortcutNode.getDecorationValue(IRDMethod.class));
        methodWriter.writePop(MethodWriter.getType(irDotSubShortcutNode.getDecorationValue(IRDMethod.class).returnType()).getSize());
    }

    @Override
    public void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreListShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreListShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irStoreListShortcutNode.getDecorationValue(IRDMethod.class));
        methodWriter.writePop(MethodWriter.getType(irStoreListShortcutNode.getDecorationValue(IRDMethod.class).returnType()).getSize());
    }

    @Override
    public void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreMapShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreMapShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irStoreMapShortcutNode.getDecorationValue(IRDMethod.class));
        methodWriter.writePop(MethodWriter.getType(irStoreMapShortcutNode.getDecorationValue(IRDMethod.class).returnType()).getSize());
    }

    @Override
    public void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        if (irStoreFieldMemberNode.hasCondition(IRCStatic.class) == false) {
            methodWriter.loadThis();
        }

        visit(irStoreFieldMemberNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreFieldMemberNode.getLocation());

        boolean isStatic = irStoreFieldMemberNode.hasCondition(IRCStatic.class);
        String memberFieldName = irStoreFieldMemberNode.getDecorationValue(IRDName.class);
        Type asmMemberFieldType = MethodWriter.getType(irStoreFieldMemberNode.getDecorationValue(IRDStoreType.class));

        if (isStatic) {
            methodWriter.putStatic(CLASS_TYPE, memberFieldName, asmMemberFieldType);
        } else {
            methodWriter.loadThis();
            methodWriter.putField(CLASS_TYPE, memberFieldName, asmMemberFieldType);
        }
    }

    @Override
    public void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreBraceDefNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreBraceDefNode.getLocation());
        Type methodType = Type.getMethodType(
            MethodWriter.getType(void.class),
            MethodWriter.getType(def.class),
            MethodWriter.getType(irStoreBraceDefNode.getDecorationValue(IRDIndexType.class)),
            MethodWriter.getType(irStoreBraceDefNode.getDecorationValue(IRDStoreType.class))
        );
        methodWriter.invokeDefCall("arrayStore", methodType, DefBootstrap.ARRAY_STORE);
    }

    @Override
    public void visitStoreBrace(StoreBraceNode irStoreBraceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreBraceNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreBraceNode.getLocation());
        methodWriter.arrayStore(MethodWriter.getType(irStoreBraceNode.getDecorationValue(IRDStoreType.class)));
    }

    @Override
    public void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallDefNode.getLocation());

        // its possible to have unknown functional interfaces
        // as arguments that require captures; the set of
        // captures with call arguments is ambiguous so
        // additional information is encoded to indicate
        // which are values are arguments and which are captures
        StringBuilder defCallRecipe = new StringBuilder();
        List<Object> boostrapArguments = new ArrayList<>();
        List<Class<?>> typeParameters = new ArrayList<>();
        int capturedCount = 0;

        // add an Object class as a placeholder type for the receiver
        typeParameters.add(Object.class);

        boolean pushScriptThis = irInvokeCallDefNode.hasCondition(IRCScriptAware.class);
        if (pushScriptThis) {
            methodWriter.loadThis();
            defCallRecipe.append('S');
            typeParameters.add(ScriptThis.class);
        }

        for (int i = 0; i < irInvokeCallDefNode.getArgumentNodes().size(); ++i) {
            ExpressionNode irArgumentNode = irInvokeCallDefNode.getArgumentNodes().get(i);
            visit(irArgumentNode, writeScope);

            typeParameters.add(irArgumentNode.getDecorationValue(IRDExpressionType.class));

            // handle the case for unknown functional interface
            // to hint at which values are the call's arguments
            // versus which values are captures
            if (irArgumentNode instanceof DefInterfaceReferenceNode defInterfaceReferenceNode) {
                List<String> captureNames = defInterfaceReferenceNode.getDecorationValueOrDefault(
                    IRDCaptureNames.class,
                    Collections.emptyList()
                );
                boostrapArguments.add(defInterfaceReferenceNode.getDecorationValue(IRDDefReferenceEncoding.class).toString());

                if (defInterfaceReferenceNode.hasCondition(IRCInstanceCapture.class)) {
                    capturedCount++;
                    typeParameters.add(ScriptThis.class);
                }

                // the encoding uses a char to indicate the number of captures
                // where the value is the number of current arguments plus the
                // total number of captures for easier capture count tracking
                // when resolved at runtime
                char encoding = (char) (i + capturedCount);
                defCallRecipe.append(encoding);
                capturedCount += captureNames.size();

                for (String captureName : captureNames) {
                    Variable captureVariable = writeScope.getVariable(captureName);
                    typeParameters.add(captureVariable.getType());
                }
            }
        }

        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            Class<?> typeParameter = typeParameters.get(index);
            if (typeParameter.equals(ScriptThis.class)) {
                asmParameterTypes[index] = CLASS_TYPE;
            } else {
                asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
            }
        }

        String methodName = irInvokeCallDefNode.getDecorationValue(IRDName.class);
        Type methodType = Type.getMethodType(
            MethodWriter.getType(irInvokeCallDefNode.getDecorationValue(IRDExpressionType.class)),
            asmParameterTypes
        );

        boostrapArguments.add(0, defCallRecipe.toString());
        methodWriter.invokeDefCall(methodName, methodType, DefBootstrap.METHOD_CALL, boostrapArguments.toArray());
    }

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallNode.getLocation());

        PainlessMethod painlessMethod = irInvokeCallNode.getMethod();

        if (irInvokeCallNode.getBox().isPrimitive()) {
            methodWriter.box(MethodWriter.getType(irInvokeCallNode.getBox()));
        }

        if (painlessMethod.annotations().containsKey(ScriptAwareAnnotation.class)) {
            methodWriter.loadThis();
        }

        for (ExpressionNode irArgumentNode : irInvokeCallNode.getArgumentNodes()) {
            visit(irArgumentNode, writeScope);
        }

        // Just before the invoke, the stack holds exactly the method's Java signature (receiver first for instance methods;
        // @script_aware/@inject_constant extras are ordinary operands). Replay those operands through the estimator and
        // charge the estimate before the allocating call runs.
        java.lang.reflect.Method methodEstimator = irInvokeCallNode.getDecorationValue(IRDAllocationEstimator.class);
        if (methodEstimator != null && isAllocationTrackingActive(writeScope)) {
            Variable[] operands = writeDynamicAllocationCheck(
                writeScope,
                methodWriter,
                "callOperand",
                painlessMethod.methodType().parameterArray(),
                methodEstimator
            );
            loadCallOperands(methodWriter, operands);
        }

        methodWriter.invokeMethodCall(painlessMethod);
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallMemberNode.getLocation());

        LocalFunction localFunction = irInvokeCallMemberNode.getDecorationValue(IRDFunction.class);
        PainlessMethod thisMethod = irInvokeCallMemberNode.getDecorationValue(IRDThisMethod.class);
        PainlessMethod importedMethod = irInvokeCallMemberNode.getDecorationValue(IRDMethod.class);
        PainlessClassBinding classBinding = irInvokeCallMemberNode.getDecorationValue(IRDClassBinding.class);
        PainlessInstanceBinding instanceBinding = irInvokeCallMemberNode.getDecorationValue(IRDInstanceBinding.class);
        List<ExpressionNode> irArgumentNodes = irInvokeCallMemberNode.getArgumentNodes();

        if (localFunction != null) {
            if (localFunction.isStatic() == false) {
                methodWriter.loadThis();
            }

            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            if (localFunction.isStatic()) {
                methodWriter.invokeStatic(CLASS_TYPE, localFunction.getAsmMethod());
            } else {
                methodWriter.invokeVirtual(CLASS_TYPE, localFunction.getAsmMethod());
            }
        } else if (thisMethod != null) {
            methodWriter.loadThis();

            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            Method asmMethod = new Method(
                thisMethod.javaMethod().getName(),
                thisMethod.methodType().dropParameterTypes(0, 1).toMethodDescriptorString()
            );
            methodWriter.invokeVirtual(CLASS_TYPE, asmMethod);
        } else if (importedMethod != null) {
            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            java.lang.reflect.Method importedEstimator = irInvokeCallMemberNode.getDecorationValue(IRDAllocationEstimator.class);
            if (importedEstimator != null && isAllocationTrackingActive(writeScope)) {
                Variable[] operands = writeDynamicAllocationCheck(
                    writeScope,
                    methodWriter,
                    "callOperand",
                    importedMethod.methodType().parameterArray(),
                    importedEstimator
                );
                loadCallOperands(methodWriter, operands);
            }

            Type asmType = Type.getType(importedMethod.targetClass());
            Method asmMethod = new Method(importedMethod.javaMethod().getName(), importedMethod.methodType().toMethodDescriptorString());
            methodWriter.invokeStatic(asmType, asmMethod);
        } else if (classBinding != null) {
            Type type = Type.getType(classBinding.javaConstructor().getDeclaringClass());
            int classBindingOffset = irInvokeCallMemberNode.hasCondition(IRCStatic.class) ? 0 : 1;
            int javaConstructorParameterCount = classBinding.javaConstructor().getParameterCount() - classBindingOffset;
            String bindingName = irInvokeCallMemberNode.getDecorationValue(IRDName.class);

            Label nonNull = new Label();

            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);
            methodWriter.ifNonNull(nonNull);
            methodWriter.loadThis();
            methodWriter.newInstance(type);
            methodWriter.dup();

            if (classBindingOffset == 1) {
                methodWriter.loadThis();
            }

            for (int argument = 0; argument < javaConstructorParameterCount; ++argument) {
                visit(irArgumentNodes.get(argument), writeScope);
            }

            methodWriter.invokeConstructor(type, Method.getMethod(classBinding.javaConstructor()));
            methodWriter.putField(CLASS_TYPE, bindingName, type);

            methodWriter.mark(nonNull);
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < classBinding.javaMethod().getParameterCount(); ++argument) {
                visit(irArgumentNodes.get(argument + javaConstructorParameterCount), writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(classBinding.javaMethod()));
        } else if (instanceBinding != null) {
            Type type = Type.getType(instanceBinding.targetInstance().getClass());
            String bindingName = irInvokeCallMemberNode.getDecorationValue(IRDName.class);

            methodWriter.loadThis();
            methodWriter.getStatic(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < instanceBinding.javaMethod().getParameterCount(); ++argument) {
                visit(irArgumentNodes.get(argument), writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(instanceBinding.javaMethod()));
        } else {
            throw new IllegalStateException("invalid unbound call");
        }
    }

    @Override
    public void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irFlipArrayIndexNode.getChildNode(), writeScope);

        Label noFlip = new Label();
        methodWriter.dup();
        methodWriter.ifZCmp(Opcodes.IFGE, noFlip);
        methodWriter.swap();
        methodWriter.dupX1();
        methodWriter.arrayLength();
        methodWriter.visitInsn(Opcodes.IADD);
        methodWriter.mark(noFlip);
    }

    @Override
    public void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irFlipCollectionIndexNode.getChildNode(), writeScope);

        Label noFlip = new Label();
        methodWriter.dup();
        methodWriter.ifZCmp(Opcodes.IFGE, noFlip);
        methodWriter.swap();
        methodWriter.dupX1();
        methodWriter.invokeInterface(WriterConstants.COLLECTION_TYPE, WriterConstants.COLLECTION_SIZE);
        methodWriter.visitInsn(Opcodes.IADD);
        methodWriter.mark(noFlip);
    }

    @Override
    public void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        methodWriter.dup();
        visit(irFlipDefIndexNode.getChildNode(), writeScope);

        Type asmExpressionType = MethodWriter.getType(irFlipDefIndexNode.getChildNode().getDecorationValue(IRDExpressionType.class));
        Type asmDefType = MethodWriter.getType(def.class);
        Type methodType = Type.getMethodType(asmExpressionType, asmDefType, asmExpressionType);
        methodWriter.invokeDefCall("normalizeIndex", methodType, DefBootstrap.INDEX_NORMALIZE);
    }

    @Override
    public void visitDup(DupNode irDupNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        visit(irDupNode.getChildNode(), writeScope);

        int size = irDupNode.getDecorationValueOrDefault(IRDSize.class, 0);
        int depth = irDupNode.getDecorationValueOrDefault(IRDDepth.class, 0);

        methodWriter.writeDup(size, depth);
    }

    // placeholder class referring to the script instance
    private static final class ScriptThis {}
}
