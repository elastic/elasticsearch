/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.api.Augmentation;
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
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.IRDecorations.IRCAllEscape;
import org.elasticsearch.painless.symbol.IRDecorations.IRCCaptureBox;
import org.elasticsearch.painless.symbol.IRDecorations.IRCContinuous;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInitialize;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInstanceCapture;
import org.elasticsearch.painless.symbol.IRDecorations.IRCStatic;
import org.elasticsearch.painless.symbol.IRDecorations.IRCSynthetic;
import org.elasticsearch.painless.symbol.IRDecorations.IRCVarArgs;
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
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;

import static org.elasticsearch.painless.WriterConstants.BASE_INTERFACE_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.EQUALS;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_HASNEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_NEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_TYPE;
import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;

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

        ClassWriter classWriter = new ClassWriter(scriptScope.getCompilerSettings(), statements, debugStream,
                scriptClassInfo.getBaseClass(), classFrames, classAccess, className, classInterfaces);
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(scriptScope.getScriptName()), null);
        writeScope = writeScope.newClassScope(classWriter);

        Method init;

        int constuctors = scriptClassInfo.getBaseClass().getConstructors().length;
        if (constuctors == 0) {
            init = new Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else if (constuctors == 1) {
            init = new Method("<init>", MethodType.methodType(void.class,
                    scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes()).toMethodDescriptorString());
        } else {
            throw new IllegalStateException("Multiple constructors are not supported");
        }

        // Write the constructor:
        MethodWriter constructor = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, init);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        constructor.returnValue();
        constructor.endMethod();

        BlockNode irClinitBlockNode = irClassNode.getClinitBlockNode();

        if (irClinitBlockNode.getStatementsNodes().isEmpty() == false) {
            MethodWriter methodWriter = classWriter.newMethodWriter(
                    Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                    new Method("<clinit>", Type.getType(void.class), new Type[0]));
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

        if (irFunctionNode.getDecorationValue(IRDMaxLoopCounter.class) > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = writeScope.defineInternalVariable(int.class, "loop");

            methodWriter.push(irFunctionNode.getDecorationValue(IRDMaxLoopCounter.class));
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        visit(irFunctionNode.getBlockNode(), writeScope.newBlockScope());

        methodWriter.endMethod();
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

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irWhileLoopNode.getLocation());
        }

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

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irDoWhileLoopNode.getLocation());
        }

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
        } else if (irInitializerNode instanceof ExpressionNode) {
            ExpressionNode irExpressionNode = (ExpressionNode)irInitializerNode;

            visit(irExpressionNode, writeScope);
            methodWriter.writePop(MethodWriter.getType(irExpressionNode.getDecorationValue(IRDExpressionType.class)).getSize());
        }

        methodWriter.mark(start);

        if (irConditionNode != null && irForLoopNode.hasCondition(IRCContinuous.class) == false) {
            visit(irConditionNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irForLoopNode.getLocation());
        }

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
                irForEachSubArrayNode.getDecorationValue(IRDVariableName.class));
        Variable array = writeScope.defineInternalVariable(
                irForEachSubArrayNode.getDecorationValue(IRDArrayType.class),
                irForEachSubArrayNode.getDecorationValue(IRDArrayName.class));
        Variable index = writeScope.defineInternalVariable(
                irForEachSubArrayNode.getDecorationValue(IRDIndexType.class),
                irForEachSubArrayNode.getDecorationValue(IRDIndexName.class));

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

        Variable variable = writeScope.defineVariable(
                irForEachSubIterableNode.getDecorationValue(IRDVariableType.class),
                irForEachSubIterableNode.getDecorationValue(IRDVariableName.class));
        Variable iterator = writeScope.defineInternalVariable(
                irForEachSubIterableNode.getDecorationValue(IRDIterableType.class),
                irForEachSubIterableNode.getDecorationValue(IRDIterableName.class));

        visit(irForEachSubIterableNode.getConditionNode(), writeScope);

        PainlessMethod painlessMethod = irForEachSubIterableNode.getDecorationValue(IRDMethod.class);

        if (painlessMethod == null) {
            Type methodType = Type.getMethodType(Type.getType(Iterator.class), Type.getType(Object.class));
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

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_NEXT);
        methodWriter.writeCast(irForEachSubIterableNode.getDecorationValue(IRDCast.class));
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

            if (sort == void.class || sort == boolean.class || sort == byte.class ||
                    sort == short.class || sort == char.class || sort == int.class) {
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
                writeScope.getTryBeginLabel(), writeScope.getTryEndLabel(), jump, variable.getAsmType().getInternalName());

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
                        throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                                "for type [" + irUnaryMathNode.getDecorationString(IRDExpressionType.class) + "]");
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
                throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                        "for type [" + irUnaryMathNode.getDecorationString(IRDExpressionType.class) + "]");
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
                throw new IllegalStateException("unexpected binary math operation [" + operation + "] " +
                        "for type [" + irBinaryMathNode.getDecorationString(IRDExpressionType.class) + "]");
            }
        } else {
            visit(irLeftNode, writeScope);
            visit(irRightNode, writeScope);

            Class<?> expressionType = irBinaryMathNode.getDecorationValue(IRDExpressionType.class);

            if (irBinaryMathNode.getDecorationValue(IRDBinaryType.class) == def.class ||
                    (irBinaryMathNode.getDecoration(IRDShiftType.class) != null &&
                            irBinaryMathNode.getDecorationValue(IRDShiftType.class) == def.class)) {
                methodWriter.writeDynamicBinaryInstruction(
                        irBinaryMathNode.getLocation(),
                        expressionType,
                        irLeftNode.getDecorationValue(IRDExpressionType.class),
                        irRightNode.getDecorationValue(IRDExpressionType.class),
                        operation,
                        irBinaryMathNode.getDecorationValueOrDefault(IRDFlags.class, 0));
            } else {
                methodWriter.writeBinaryInstruction(irBinaryMathNode.getLocation(), expressionType, operation);
            }
        }
    }

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irStringConcatenationNode.getLocation());
        methodWriter.writeNewStrings();

        for (ExpressionNode argumentNode : irStringConcatenationNode.getArgumentNodes()) {
            visit(argumentNode, writeScope);
            methodWriter.writeAppendStrings(argumentNode.getDecorationValue(IRDExpressionType.class));
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
            throw new IllegalStateException("unexpected boolean operation [" + operation + "] " +
                    "for type [" + irBooleanNode.getDecorationString(IRDExpressionType.class) + "]");
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
        boolean lt  = operation == Operation.LT;
        boolean lte = operation == Operation.LTE;
        boolean gt  = operation == Operation.GT;
        boolean gte = operation == Operation.GTE;

        boolean writejump = true;

        Class<?> comparisonType = irComparisonNode.getDecorationValue(IRDComparisonType.class);
        Type type = MethodWriter.getType(comparisonType);

        if (comparisonType == void.class || comparisonType == byte.class
                || comparisonType == short.class || comparisonType == char.class) {
            throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                    "for type [" + irComparisonNode.getDecorationString(IRDExpressionType.class) + "]");
        } else if (comparisonType == boolean.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecorationString(IRDExpressionType.class) + "]");
            }
        } else if (comparisonType == int.class || comparisonType == long.class
                || comparisonType == float.class || comparisonType == double.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else if (lt) methodWriter.ifCmp(type, MethodWriter.LT, jump);
            else if (lte) methodWriter.ifCmp(type, MethodWriter.LE, jump);
            else if (gt) methodWriter.ifCmp(type, MethodWriter.GT, jump);
            else if (gte) methodWriter.ifCmp(type, MethodWriter.GE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecorationString(IRDExpressionType.class) + "]");
            }

        } else if (comparisonType == def.class) {
            Type booleanType = Type.getType(boolean.class);
            Type descriptor = Type.getMethodType(booleanType,
                    MethodWriter.getType(irLeftNode.getDecorationValue(IRDExpressionType.class)),
                    MethodWriter.getType(irRightNode.getDecorationValue(IRDExpressionType.class)));

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
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecorationString(IRDExpressionType.class) + "]");
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
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecorationString(IRDExpressionType.class) + "]");
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
        methodWriter.writeCast(irCastNode.getDecorationValue(IRDCast.class));
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
                Type.getType(painlessConstructor.javaConstructor.getDeclaringClass()),
                Method.getMethod(painlessConstructor.javaConstructor));

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
                Type.getType(painlessConstructor.javaConstructor.getDeclaringClass()),
                Method.getMethod(painlessConstructor.javaConstructor));

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
            methodWriter.push(irNewArrayNode.getArgumentNodes().size());
            methodWriter.newArray(MethodWriter.getType(expressionType.getComponentType()));

            for (int index = 0; index < irArgumentNodes.size(); ++index) {
                ExpressionNode irArgumentNode = irArgumentNodes.get(index);

                methodWriter.dup();
                methodWriter.push(index);
                visit(irArgumentNode, writeScope);
                methodWriter.arrayStore(MethodWriter.getType(expressionType.getComponentType()));
            }
        } else {
            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            if (irArgumentNodes.size() > 1) {
                methodWriter.visitMultiANewArrayInsn(
                        MethodWriter.getType(expressionType).getDescriptor(),
                        irArgumentNodes.size());
            } else {
                methodWriter.newArray(MethodWriter.getType(expressionType.getComponentType()));
            }
        }
    }

    @Override
    public void visitNewObject(NewObjectNode irNewObjectNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNewObjectNode.getLocation());

        methodWriter.newInstance(MethodWriter.getType(irNewObjectNode.getDecorationValue(IRDExpressionType.class)));

        // Always dup so that visitStatementExpression's always has something to pop
        methodWriter.dup();

        for (ExpressionNode irArgumentNode : irNewObjectNode.getArgumentNodes()) {
            visit(irArgumentNode, writeScope);
        }

        PainlessConstructor painlessConstructor = irNewObjectNode.getDecorationValue(IRDConstructor.class);
        methodWriter.invokeConstructor(
                Type.getType(painlessConstructor.javaConstructor.getDeclaringClass()),
                Method.getMethod(painlessConstructor.javaConstructor));
    }

    @Override
    public void visitConstant(ConstantNode irConstantNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        Object constant = irConstantNode.getDecorationValue(IRDConstant.class);

        if      (constant instanceof String)    methodWriter.push((String)constant);
        else if (constant instanceof Double)    methodWriter.push((double)constant);
        else if (constant instanceof Float)     methodWriter.push((float)constant);
        else if (constant instanceof Long)      methodWriter.push((long)constant);
        else if (constant instanceof Integer)   methodWriter.push((int)constant);
        else if (constant instanceof Character) methodWriter.push((char)constant);
        else if (constant instanceof Short)     methodWriter.push((short)constant);
        else if (constant instanceof Byte)      methodWriter.push((byte)constant);
        else if (constant instanceof Boolean)   methodWriter.push((boolean)constant);
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
        methodWriter.push((String)null);

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

        if (irTypedInterfaceReferenceNode.hasCondition(IRCInstanceCapture.class)) {
            Variable capturedThis = writeScope.getInternalVariable("this");
            methodWriter.visitVarInsn(CLASS_TYPE.getOpcode(Opcodes.ILOAD), capturedThis.getSlot());
        }

        List<String> captureNames = irTypedInterfaceReferenceNode.getDecorationValue(IRDCaptureNames.class);
        boolean captureBox = irTypedInterfaceReferenceNode.hasCondition(IRCCaptureBox.class);

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
                MethodWriter.getType(def.class));
        methodWriter.invokeDefCall(irLoadDotDefNode.getDecorationValue(IRDValue.class), methodType, DefBootstrap.LOAD);
    }

    @Override
    public void visitLoadDot(LoadDotNode irLoadDotNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotNode.getLocation());

        PainlessField painlessField = irLoadDotNode.getDecorationValue(IRDField.class);
        boolean isStatic = Modifier.isStatic(painlessField.javaField.getModifiers());
        Type asmOwnerType = Type.getType(painlessField.javaField.getDeclaringClass());
        String fieldName = painlessField.javaField.getName();
        Type asmFieldType = MethodWriter.getType(painlessField.typeParameter);

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

        if (getterPainlessMethod.returnType.equals(getterPainlessMethod.javaMethod.getReturnType()) == false) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType));
        }
    }

    @Override
    public void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadListShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irLoadListShortcutNode.getDecorationValue(IRDMethod.class);
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType == getterPainlessMethod.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType));
        }
    }

    @Override
    public void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadMapShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irLoadMapShortcutNode.getDecorationValue(IRDMethod.class);
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType != getterPainlessMethod.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType));
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
                MethodWriter.getType(irLoadBraceDefNode.getDecorationValue(IRDIndexType.class)));
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
                MethodWriter.getType(irStoreDotDefNode.getDecorationValue(IRDStoreType.class)));
        methodWriter.invokeDefCall(irStoreDotDefNode.getDecorationValue(IRDValue.class), methodType, DefBootstrap.STORE);
    }

    @Override
    public void visitStoreDot(StoreDotNode irStoreDotNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreDotNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreDotNode.getLocation());

        PainlessField painlessField = irStoreDotNode.getDecorationValue(IRDField.class);
        boolean isStatic = Modifier.isStatic(painlessField.javaField.getModifiers());
        Type asmOwnerType = Type.getType(painlessField.javaField.getDeclaringClass());
        String fieldName = painlessField.javaField.getName();
        Type asmFieldType = MethodWriter.getType(painlessField.typeParameter);

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
        methodWriter.writePop(MethodWriter.getType(irDotSubShortcutNode.getDecorationValue(IRDMethod.class).returnType).getSize());
    }

    @Override
    public void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreListShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreListShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irStoreListShortcutNode.getDecorationValue(IRDMethod.class));
        methodWriter.writePop(MethodWriter.getType(irStoreListShortcutNode.getDecorationValue(IRDMethod.class).returnType).getSize());
    }

    @Override
    public void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreMapShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreMapShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irStoreMapShortcutNode.getDecorationValue(IRDMethod.class));
        methodWriter.writePop(MethodWriter.getType(irStoreMapShortcutNode.getDecorationValue(IRDMethod.class).returnType).getSize());
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
                MethodWriter.getType(irStoreBraceDefNode.getDecorationValue(IRDStoreType.class)));
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

        for (int i = 0; i < irInvokeCallDefNode.getArgumentNodes().size(); ++i) {
            ExpressionNode irArgumentNode = irInvokeCallDefNode.getArgumentNodes().get(i);
            visit(irArgumentNode, writeScope);

            typeParameters.add(irArgumentNode.getDecorationValue(IRDExpressionType.class));

            // handle the case for unknown functional interface
            // to hint at which values are the call's arguments
            // versus which values are captures
            if (irArgumentNode instanceof DefInterfaceReferenceNode) {
                DefInterfaceReferenceNode defInterfaceReferenceNode = (DefInterfaceReferenceNode)irArgumentNode;
                List<String> captureNames =
                        defInterfaceReferenceNode.getDecorationValueOrDefault(IRDCaptureNames.class, Collections.emptyList());
                boostrapArguments.add(defInterfaceReferenceNode.getDecorationValue(IRDDefReferenceEncoding.class).toString());

                if (defInterfaceReferenceNode.hasCondition(IRCInstanceCapture.class)) {
                    capturedCount++;
                    typeParameters.add(ScriptThis.class);
                }

                // the encoding uses a char to indicate the number of captures
                // where the value is the number of current arguments plus the
                // total number of captures for easier capture count tracking
                // when resolved at runtime
                char encoding = (char)(i + capturedCount);
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
        Type methodType = Type.getMethodType(MethodWriter.getType(
                irInvokeCallDefNode.getDecorationValue(IRDExpressionType.class)), asmParameterTypes);

        boostrapArguments.add(0, defCallRecipe.toString());
        methodWriter.invokeDefCall(methodName, methodType, DefBootstrap.METHOD_CALL, boostrapArguments.toArray());
    }

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallNode.getLocation());

        if (irInvokeCallNode.getBox().isPrimitive()) {
            methodWriter.box(MethodWriter.getType(irInvokeCallNode.getBox()));
        }

        for (ExpressionNode irArgumentNode : irInvokeCallNode.getArgumentNodes()) {
            visit(irArgumentNode, writeScope);
        }

        methodWriter.invokeMethodCall(irInvokeCallNode.getMethod());
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

            Method asmMethod = new Method(thisMethod.javaMethod.getName(),
                    thisMethod.methodType.dropParameterTypes(0, 1).toMethodDescriptorString());
            methodWriter.invokeVirtual(CLASS_TYPE, asmMethod);
        } else if (importedMethod != null) {
            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            Type asmType = Type.getType(importedMethod.targetClass);
            Method asmMethod = new Method(importedMethod.javaMethod.getName(), importedMethod.methodType.toMethodDescriptorString());
            methodWriter.invokeStatic(asmType, asmMethod);
        } else if (classBinding != null) {
            Type type = Type.getType(classBinding.javaConstructor.getDeclaringClass());
            int classBindingOffset = irInvokeCallMemberNode.hasCondition(IRCStatic.class) ? 0 : 1;
            int javaConstructorParameterCount = classBinding.javaConstructor.getParameterCount() - classBindingOffset;
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

            methodWriter.invokeConstructor(type, Method.getMethod(classBinding.javaConstructor));
            methodWriter.putField(CLASS_TYPE, bindingName, type);

            methodWriter.mark(nonNull);
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < classBinding.javaMethod.getParameterCount(); ++argument) {
                visit(irArgumentNodes.get(argument + javaConstructorParameterCount), writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(classBinding.javaMethod));
        } else if (instanceBinding != null) {
            Type type = Type.getType(instanceBinding.targetInstance.getClass());
            String bindingName = irInvokeCallMemberNode.getDecorationValue(IRDName.class);

            methodWriter.loadThis();
            methodWriter.getStatic(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < instanceBinding.javaMethod.getParameterCount(); ++argument) {
                visit(irArgumentNodes.get(argument), writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(instanceBinding.javaMethod));
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
