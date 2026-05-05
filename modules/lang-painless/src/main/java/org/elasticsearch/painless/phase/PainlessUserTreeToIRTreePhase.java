/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.PainlessError;
import org.elasticsearch.painless.PainlessExplainError;
import org.elasticsearch.painless.PainlessWrappedException;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.ScriptClassInfo.MethodArgument;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.InvokeCallDefNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.LoadFieldMemberNode;
import org.elasticsearch.painless.ir.LoadVariableNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.ECallLocal;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.symbol.Decorations.Converter;
import org.elasticsearch.painless.symbol.Decorations.IRNodeDecoration;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.ThisPainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.script.ScriptException;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Method;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PainlessUserTreeToIRTreePhase extends DefaultUserTreeToIRTreePhase {

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();

        // This injects additional ir nodes required for
        // the "execute" method. This includes injection of ir nodes
        // to convert get methods into local variables for those
        // that are used and adds additional sandboxing by wrapping
        // the main "execute" block with several exceptions.
        if ("execute".equals(functionName)) {
            ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();
            LocalFunction localFunction = scriptScope.getFunctionTable()
                .getFunction(functionName, scriptClassInfo.getExecuteArguments().size());
            Class<?> returnType = localFunction.getReturnType();

            boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);
            BlockNode irBlockNode = (BlockNode) visit(userFunctionNode.getBlockNode(), scriptScope);

            if (methodEscape == false) {
                ExpressionNode irExpressionNode;

                if (returnType == void.class) {
                    irExpressionNode = null;
                } else {
                    if (returnType.isPrimitive()) {
                        ConstantNode irConstantNode = new ConstantNode(userFunctionNode.getLocation());
                        irConstantNode.setExpressionType(returnType);

                        if (returnType == boolean.class) {
                            irConstantNode.setConstant(false);
                        } else if (returnType == byte.class
                            || returnType == char.class
                            || returnType == short.class
                            || returnType == int.class) {
                                irConstantNode.setConstant(0);
                            } else if (returnType == long.class) {
                                irConstantNode.setConstant(0L);
                            } else if (returnType == float.class) {
                                irConstantNode.setConstant(0f);
                            } else if (returnType == double.class) {
                                irConstantNode.setConstant(0d);
                            } else {
                                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
                            }

                        irExpressionNode = irConstantNode;
                    } else {
                        irExpressionNode = new NullNode(userFunctionNode.getLocation());
                        irExpressionNode.setExpressionType(returnType);
                    }
                }

                ReturnNode irReturnNode = new ReturnNode(userFunctionNode.getLocation());
                irReturnNode.setExpressionNode(irExpressionNode);

                irBlockNode.addStatementNode(irReturnNode);
            }

            List<String> parameterNames = scriptClassInfo.getExecuteArguments().stream().map(MethodArgument::name).toList();

            FunctionNode irFunctionNode = new FunctionNode(userFunctionNode.getLocation());
            irFunctionNode.setBlockNode(irBlockNode);
            irFunctionNode.setName("execute");
            irFunctionNode.setReturnType(returnType);
            irFunctionNode.setTypeParameters(localFunction.getTypeParameters());
            irFunctionNode.setParameterNames(parameterNames);
            irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());

            injectStaticFieldsAndGetters();
            injectGetsDeclarations(irBlockNode, scriptScope);
            injectNeedsMethods(scriptScope);
            injectSandboxExceptions(irFunctionNode);

            scriptScope.putDecoration(userFunctionNode, new IRNodeDecoration(irFunctionNode));
        } else {
            super.visitFunction(userFunctionNode, scriptScope);
        }
    }

    // adds static fields and getter methods required by PainlessScript for exception handling
    protected void injectStaticFieldsAndGetters() {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectStaticFieldsAndGetters", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(String.class);
        irFieldNode.setName("$NAME");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(String.class);
        irFieldNode.setName("$SOURCE");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(BitSet.class);
        irFieldNode.setName("$STATEMENTS");

        irClassNode.addFieldNode(irFieldNode);

        FunctionNode irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.setName("getName");
        irFunctionNode.setReturnType(String.class);
        irFunctionNode.setTypeParameters(List.of());
        irFunctionNode.setParameterNames(List.of());
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        BlockNode irBlockNode = new BlockNode(internalLocation);
        irBlockNode.setAllEscape(true);

        irFunctionNode.setBlockNode(irBlockNode);

        ReturnNode irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.setExpressionType(String.class);
        irLoadFieldMemberNode.setName("$NAME");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.setName("getSource");
        irFunctionNode.setReturnType(String.class);
        irFunctionNode.setTypeParameters(List.of());
        irFunctionNode.setParameterNames(List.of());
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode(internalLocation);
        irBlockNode.setAllEscape(true);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.setExpressionType(String.class);
        irLoadFieldMemberNode.setName("$SOURCE");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.setName("getStatements");
        irFunctionNode.setReturnType(BitSet.class);
        irFunctionNode.setTypeParameters(List.of());
        irFunctionNode.setParameterNames(List.of());
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode(internalLocation);
        irBlockNode.setAllEscape(true);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.setExpressionType(BitSet.class);
        irLoadFieldMemberNode.setName("$STATEMENTS");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);
    }

    // convert gets methods to a new set of inserted ir nodes as necessary -
    // requires the gets method name be modified from "getExample" to "example"
    // if a get method variable isn't used it's declaration node is removed from
    // the ir tree permanently so there is no frivolous variable slotting
    protected static void injectGetsDeclarations(BlockNode irBlockNode, ScriptScope scriptScope) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectGetsDeclarations", 0);

        for (int i = 0; i < scriptScope.getScriptClassInfo().getGetMethods().size(); ++i) {
            Method getMethod = scriptScope.getScriptClassInfo().getGetMethods().get(i);
            String name = getMethod.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            if (scriptScope.getUsedVariables().contains(name)) {
                Class<?> returnType = scriptScope.getScriptClassInfo().getGetReturns().get(i);

                DeclarationNode irDeclarationNode = new DeclarationNode(internalLocation);
                irDeclarationNode.setName(name);
                irDeclarationNode.setDeclarationType(returnType);
                irBlockNode.getStatementsNodes().add(0, irDeclarationNode);

                InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
                irInvokeCallMemberNode.setExpressionType(returnType);
                irInvokeCallMemberNode.setFunction(new LocalFunction(getMethod.getName(), returnType, List.of(), true, false));
                irDeclarationNode.setExpressionNode(irInvokeCallMemberNode);
            }
        }
    }

    // injects needs methods as defined by ScriptClassInfo
    protected void injectNeedsMethods(ScriptScope scriptScope) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectNeedsMethods", 0);

        for (org.objectweb.asm.commons.Method needsMethod : scriptScope.getScriptClassInfo().getNeedsMethods()) {
            String name = needsMethod.getName();
            name = name.substring(5);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            FunctionNode irFunctionNode = new FunctionNode(internalLocation);
            irFunctionNode.setName(needsMethod.getName());
            irFunctionNode.setReturnType(boolean.class);
            irFunctionNode.setTypeParameters(List.of());
            irFunctionNode.setParameterNames(List.of());
            irFunctionNode.setSynthetic(true);
            irFunctionNode.setMaxLoopCounter(0);

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode irBlockNode = new BlockNode(internalLocation);
            irBlockNode.setAllEscape(true);

            irFunctionNode.setBlockNode(irBlockNode);

            ReturnNode irReturnNode = new ReturnNode(internalLocation);

            irBlockNode.addStatementNode(irReturnNode);

            ConstantNode irConstantNode = new ConstantNode(internalLocation);
            irConstantNode.setExpressionType(boolean.class);
            irConstantNode.setConstant(scriptScope.getUsedVariables().contains(name));

            irReturnNode.setExpressionNode(irConstantNode);
        }
    }

    /*
     * Decorate the execute method with nodes to wrap the user statements with
     * the sandboxed errors as follows:
     *
     * } catch (PainlessExplainError e) {
     *     throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
     * }
     *
     * and
     *
     * } catch (SecurityException e) {
     *     throw e;
     * }
     *
     * and
     *
     * } catch (PainlessError | LinkageError | OutOfMemoryError | StackOverflowError | Exception e) {
     *     throw this.convertToScriptException(e, e.getHeaders())
     * }
     *
     */
    protected static void injectSandboxExceptions(FunctionNode irFunctionNode) {
        try {
            Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectSandboxExceptions", 0);
            BlockNode irBlockNode = irFunctionNode.getBlockNode();

            TryNode irTryNode = new TryNode(internalLocation);
            irTryNode.setBlockNode(irBlockNode);

            ThrowNode irThrowNode = createCatchAndThrow(PainlessExplainError.class, internalLocation, irTryNode);

            InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
            irInvokeCallMemberNode.setExpressionType(ScriptException.class);
            irInvokeCallMemberNode.setFunction(
                new LocalFunction("convertToScriptException", ScriptException.class, List.of(Throwable.class, Map.class), true, false)
            );

            irThrowNode.setExpressionNode(irInvokeCallMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(ScriptException.class);
            irLoadVariableNode.setName("#painlessExplainError");

            irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(internalLocation);
            irBinaryImplNode.setExpressionType(Map.class);

            irInvokeCallMemberNode.addArgumentNode(irBinaryImplNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(PainlessExplainError.class);
            irLoadVariableNode.setName("#painlessExplainError");

            irBinaryImplNode.setLeftNode(irLoadVariableNode);

            InvokeCallNode irInvokeCallNode = new InvokeCallNode(internalLocation);
            irInvokeCallNode.setExpressionType(Map.class);
            irInvokeCallNode.setBox(PainlessExplainError.class);
            irInvokeCallNode.setMethod(
                new PainlessMethod(
                    PainlessExplainError.class.getMethod("getHeaders", PainlessLookup.class),
                    PainlessExplainError.class,
                    null,
                    List.of(),
                    null,
                    null,
                    Map.of()
                )
            );

            irBinaryImplNode.setRightNode(irInvokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.setExpressionType(PainlessLookup.class);
            irLoadFieldMemberNode.setName("$DEFINITION");
            irLoadFieldMemberNode.setStatic(true);

            irInvokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irThrowNode = createCatchAndThrow(SecurityException.class, internalLocation, irTryNode);
            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(SecurityException.class);
            irLoadVariableNode.setName(getExceptionVariableName(SecurityException.class));
            irThrowNode.setExpressionNode(irLoadVariableNode);

            for (Class<? extends Throwable> throwable : List.of(
                PainlessError.class,
                PainlessWrappedException.class,
                LinkageError.class,
                OutOfMemoryError.class,
                StackOverflowError.class,
                Exception.class
            )) {

                irThrowNode = createCatchAndThrow(throwable, internalLocation, irTryNode);

                irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
                irInvokeCallMemberNode.setExpressionType(ScriptException.class);
                irInvokeCallMemberNode.setFunction(
                    new LocalFunction("convertToScriptException", ScriptException.class, List.of(Throwable.class, Map.class), true, false)
                );

                irThrowNode.setExpressionNode(irInvokeCallMemberNode);

                irLoadVariableNode = new LoadVariableNode(internalLocation);
                irLoadVariableNode.setExpressionType(ScriptException.class);
                irLoadVariableNode.setName(getExceptionVariableName(throwable));

                irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

                irBinaryImplNode = new BinaryImplNode(internalLocation);
                irBinaryImplNode.setExpressionType(Map.class);

                irInvokeCallMemberNode.addArgumentNode(irBinaryImplNode);

                StaticNode irStaticNode = new StaticNode(internalLocation);
                irStaticNode.setExpressionType(Collections.class);

                irBinaryImplNode.setLeftNode(irStaticNode);

                irInvokeCallNode = new InvokeCallNode(internalLocation);
                irInvokeCallNode.setExpressionType(Map.class);
                irInvokeCallNode.setBox(Collections.class);
                irInvokeCallNode.setMethod(
                    new PainlessMethod(Collections.class.getMethod("emptyMap"), Collections.class, null, List.of(), null, null, Map.of())
                );

                irBinaryImplNode.setRightNode(irInvokeCallNode);
            }

            irBlockNode = new BlockNode(internalLocation);
            irBlockNode.setAllEscape(true);
            irBlockNode.addStatementNode(irTryNode);

            irFunctionNode.setBlockNode(irBlockNode);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    // Turn an exception class name into a local variable name to hold an object of that type
    private static String getExceptionVariableName(Class<? extends Throwable> throwable) {
        String name = throwable.getSimpleName();
        return "#" + Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }

    // Helper for catching a known exception type. Returns the ThrowNode to be filled in.
    private static ThrowNode createCatchAndThrow(Class<? extends Throwable> throwable, Location internalLocation, TryNode irTryNode) {
        String name = getExceptionVariableName(throwable);

        CatchNode irCatchNode = new CatchNode(internalLocation);
        irCatchNode.setExceptionType(throwable);
        irCatchNode.setSymbol(name);

        irTryNode.addCatchNode(irCatchNode);

        BlockNode irCatchBlockNode = new BlockNode(internalLocation);
        irCatchBlockNode.setAllEscape(true);

        irCatchNode.setBlockNode(irCatchBlockNode);
        ThrowNode irThrowNode = new ThrowNode(internalLocation);
        irCatchBlockNode.addStatementNode(irThrowNode);

        return irThrowNode;
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        // sets IRNodeDecoration with ReturnNode or StatementExpressionNode
        super.visitExpression(userExpressionNode, scriptScope);
        injectConverter(userExpressionNode, scriptScope);
    }

    @Override
    public void visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        super.visitReturn(userReturnNode, scriptScope);
        injectConverter(userReturnNode, scriptScope);
    }

    public static void injectConverter(AStatement userStatementNode, ScriptScope scriptScope) {
        Converter converter = scriptScope.getDecoration(userStatementNode, Converter.class);
        if (converter == null) {
            return;
        }

        IRNodeDecoration irNodeDecoration = scriptScope.getDecoration(userStatementNode, IRNodeDecoration.class);
        IRNode irNode = irNodeDecoration.irNode();

        if ((irNode instanceof ReturnNode) == false) {
            // Shouldn't have a Converter decoration if StatementExpressionNode, should be ReturnNode if explicit return
            throw userStatementNode.createError(new IllegalStateException("illegal tree structure"));
        }

        ReturnNode returnNode = (ReturnNode) irNode;

        // inject converter
        InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(userStatementNode.getLocation());
        irInvokeCallMemberNode.setFunction(converter.converter());
        ExpressionNode returnExpression = returnNode.getExpressionNode();
        returnNode.setExpressionNode(irInvokeCallMemberNode);
        irInvokeCallMemberNode.addArgumentNode(returnExpression);
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, ScriptScope scriptScope) {
        if ("$".equals(userCallLocalNode.getMethodName())) {
            PainlessMethod thisMethod = scriptScope.getDecoration(userCallLocalNode, ThisPainlessMethod.class).thisPainlessMethod();

            InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(userCallLocalNode.getLocation());
            irInvokeCallMemberNode.setThisMethod(thisMethod);
            irInvokeCallMemberNode.addArgumentNode(injectCast(userCallLocalNode.getArgumentNodes().get(0), scriptScope));
            irInvokeCallMemberNode.setExpressionType(def.class);

            InvokeCallDefNode irCallSubDefNode = new InvokeCallDefNode(userCallLocalNode.getLocation());
            irCallSubDefNode.addArgumentNode(injectCast(userCallLocalNode.getArgumentNodes().get(1), scriptScope));
            irCallSubDefNode.setExpressionType(def.class);
            irCallSubDefNode.setName("get");

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(userCallLocalNode.getLocation());
            irBinaryImplNode.setLeftNode(irInvokeCallMemberNode);
            irBinaryImplNode.setRightNode(irCallSubDefNode);
            irBinaryImplNode.setExpressionType(def.class);

            scriptScope.putDecoration(userCallLocalNode, new IRNodeDecoration(irBinaryImplNode));
        } else {
            super.visitCallLocal(userCallLocalNode, scriptScope);
        }
    }
}
