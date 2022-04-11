/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.PainlessError;
import org.elasticsearch.painless.PainlessExplainError;
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
import org.elasticsearch.painless.symbol.IRDecorations.IRCAllEscape;
import org.elasticsearch.painless.symbol.IRDecorations.IRCStatic;
import org.elasticsearch.painless.symbol.IRDecorations.IRCSynthetic;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstant;
import org.elasticsearch.painless.symbol.IRDecorations.IRDDeclarationType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExceptionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFieldType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFunction;
import org.elasticsearch.painless.symbol.IRDecorations.IRDMaxLoopCounter;
import org.elasticsearch.painless.symbol.IRDecorations.IRDModifiers;
import org.elasticsearch.painless.symbol.IRDecorations.IRDName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDParameterNames;
import org.elasticsearch.painless.symbol.IRDecorations.IRDReturnType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDSymbol;
import org.elasticsearch.painless.symbol.IRDecorations.IRDThisMethod;
import org.elasticsearch.painless.symbol.IRDecorations.IRDTypeParameters;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.script.ScriptException;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.Arrays;
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
                        irConstantNode.attachDecoration(new IRDExpressionType(returnType));

                        if (returnType == boolean.class) {
                            irConstantNode.attachDecoration(new IRDConstant(false));
                        } else if (returnType == byte.class
                            || returnType == char.class
                            || returnType == short.class
                            || returnType == int.class) {
                                irConstantNode.attachDecoration(new IRDConstant(0));
                            } else if (returnType == long.class) {
                                irConstantNode.attachDecoration(new IRDConstant(0L));
                            } else if (returnType == float.class) {
                                irConstantNode.attachDecoration(new IRDConstant(0f));
                            } else if (returnType == double.class) {
                                irConstantNode.attachDecoration(new IRDConstant(0d));
                            } else {
                                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
                            }

                        irExpressionNode = irConstantNode;
                    } else {
                        irExpressionNode = new NullNode(userFunctionNode.getLocation());
                        irExpressionNode.attachDecoration(new IRDExpressionType(returnType));
                    }
                }

                ReturnNode irReturnNode = new ReturnNode(userFunctionNode.getLocation());
                irReturnNode.setExpressionNode(irExpressionNode);

                irBlockNode.addStatementNode(irReturnNode);
            }

            List<String> parameterNames = new ArrayList<>(scriptClassInfo.getExecuteArguments().size());

            for (MethodArgument methodArgument : scriptClassInfo.getExecuteArguments()) {
                parameterNames.add(methodArgument.getName());
            }

            FunctionNode irFunctionNode = new FunctionNode(userFunctionNode.getLocation());
            irFunctionNode.setBlockNode(irBlockNode);
            irFunctionNode.attachDecoration(new IRDName("execute"));
            irFunctionNode.attachDecoration(new IRDReturnType(returnType));
            irFunctionNode.attachDecoration(new IRDTypeParameters(new ArrayList<>(localFunction.getTypeParameters())));
            irFunctionNode.attachDecoration(new IRDParameterNames(new ArrayList<>(parameterNames)));
            irFunctionNode.attachDecoration(new IRDMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter()));

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
        irFieldNode.attachDecoration(new IRDModifiers(modifiers));
        irFieldNode.attachDecoration(new IRDFieldType(String.class));
        irFieldNode.attachDecoration(new IRDName("$NAME"));

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.attachDecoration(new IRDModifiers(modifiers));
        irFieldNode.attachDecoration(new IRDFieldType(String.class));
        irFieldNode.attachDecoration(new IRDName("$SOURCE"));

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.attachDecoration(new IRDModifiers(modifiers));
        irFieldNode.attachDecoration(new IRDFieldType(BitSet.class));
        irFieldNode.attachDecoration(new IRDName("$STATEMENTS"));

        irClassNode.addFieldNode(irFieldNode);

        FunctionNode irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.attachDecoration(new IRDName("getName"));
        irFunctionNode.attachDecoration(new IRDReturnType(String.class));
        irFunctionNode.attachDecoration(new IRDTypeParameters(Collections.emptyList()));
        irFunctionNode.attachDecoration(new IRDParameterNames(Collections.emptyList()));
        irFunctionNode.attachCondition(IRCSynthetic.class);
        irFunctionNode.attachDecoration(new IRDMaxLoopCounter(0));

        irClassNode.addFunctionNode(irFunctionNode);

        BlockNode irBlockNode = new BlockNode(internalLocation);
        irBlockNode.attachCondition(IRCAllEscape.class);

        irFunctionNode.setBlockNode(irBlockNode);

        ReturnNode irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(String.class));
        irLoadFieldMemberNode.attachDecoration(new IRDName("$NAME"));
        irLoadFieldMemberNode.attachCondition(IRCStatic.class);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.attachDecoration(new IRDName("getSource"));
        irFunctionNode.attachDecoration(new IRDReturnType(String.class));
        irFunctionNode.attachDecoration(new IRDTypeParameters(Collections.emptyList()));
        irFunctionNode.attachDecoration(new IRDParameterNames(Collections.emptyList()));
        irFunctionNode.attachCondition(IRCSynthetic.class);
        irFunctionNode.attachDecoration(new IRDMaxLoopCounter(0));

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode(internalLocation);
        irBlockNode.attachCondition(IRCAllEscape.class);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(String.class));
        irLoadFieldMemberNode.attachDecoration(new IRDName("$SOURCE"));
        irLoadFieldMemberNode.attachCondition(IRCStatic.class);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.attachDecoration(new IRDName("getStatements"));
        irFunctionNode.attachDecoration(new IRDReturnType(BitSet.class));
        irFunctionNode.attachDecoration(new IRDTypeParameters(Collections.emptyList()));
        irFunctionNode.attachDecoration(new IRDParameterNames(Collections.emptyList()));
        irFunctionNode.attachCondition(IRCSynthetic.class);
        irFunctionNode.attachDecoration(new IRDMaxLoopCounter(0));

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode(internalLocation);
        irBlockNode.attachCondition(IRCAllEscape.class);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(BitSet.class));
        irLoadFieldMemberNode.attachDecoration(new IRDName("$STATEMENTS"));
        irLoadFieldMemberNode.attachCondition(IRCStatic.class);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);
    }

    // convert gets methods to a new set of inserted ir nodes as necessary -
    // requires the gets method name be modified from "getExample" to "example"
    // if a get method variable isn't used it's declaration node is removed from
    // the ir tree permanently so there is no frivolous variable slotting
    protected void injectGetsDeclarations(BlockNode irBlockNode, ScriptScope scriptScope) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectGetsDeclarations", 0);

        for (int i = 0; i < scriptScope.getScriptClassInfo().getGetMethods().size(); ++i) {
            Method getMethod = scriptScope.getScriptClassInfo().getGetMethods().get(i);
            String name = getMethod.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            if (scriptScope.getUsedVariables().contains(name)) {
                Class<?> returnType = scriptScope.getScriptClassInfo().getGetReturns().get(i);

                DeclarationNode irDeclarationNode = new DeclarationNode(internalLocation);
                irDeclarationNode.attachDecoration(new IRDName(name));
                irDeclarationNode.attachDecoration(new IRDDeclarationType(returnType));
                irBlockNode.getStatementsNodes().add(0, irDeclarationNode);

                InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
                irInvokeCallMemberNode.attachDecoration(new IRDExpressionType(returnType));
                irInvokeCallMemberNode.attachDecoration(
                    new IRDFunction(new LocalFunction(getMethod.getName(), returnType, Collections.emptyList(), true, false))
                );
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
            irFunctionNode.attachDecoration(new IRDName(needsMethod.getName()));
            irFunctionNode.attachDecoration(new IRDReturnType(boolean.class));
            irFunctionNode.attachDecoration(new IRDTypeParameters(Collections.emptyList()));
            irFunctionNode.attachDecoration(new IRDParameterNames(Collections.emptyList()));
            irFunctionNode.attachCondition(IRCSynthetic.class);
            irFunctionNode.attachDecoration(new IRDMaxLoopCounter(0));

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode irBlockNode = new BlockNode(internalLocation);
            irBlockNode.attachCondition(IRCAllEscape.class);

            irFunctionNode.setBlockNode(irBlockNode);

            ReturnNode irReturnNode = new ReturnNode(internalLocation);

            irBlockNode.addStatementNode(irReturnNode);

            ConstantNode irConstantNode = new ConstantNode(internalLocation);
            irConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
            irConstantNode.attachDecoration(new IRDConstant(scriptScope.getUsedVariables().contains(name)));

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
    protected void injectSandboxExceptions(FunctionNode irFunctionNode) {
        try {
            Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectSandboxExceptions", 0);
            BlockNode irBlockNode = irFunctionNode.getBlockNode();

            TryNode irTryNode = new TryNode(internalLocation);
            irTryNode.setBlockNode(irBlockNode);

            ThrowNode irThrowNode = createCatchAndThrow(PainlessExplainError.class, internalLocation, irTryNode);

            InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
            irInvokeCallMemberNode.attachDecoration(new IRDExpressionType(ScriptException.class));
            irInvokeCallMemberNode.attachDecoration(
                new IRDFunction(
                    new LocalFunction(
                        "convertToScriptException",
                        ScriptException.class,
                        Arrays.asList(Throwable.class, Map.class),
                        true,
                        false
                    )
                )
            );

            irThrowNode.setExpressionNode(irInvokeCallMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(ScriptException.class));
            irLoadVariableNode.attachDecoration(new IRDName("#painlessExplainError"));

            irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(internalLocation);
            irBinaryImplNode.attachDecoration(new IRDExpressionType(Map.class));

            irInvokeCallMemberNode.addArgumentNode(irBinaryImplNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(PainlessExplainError.class));
            irLoadVariableNode.attachDecoration(new IRDName("#painlessExplainError"));

            irBinaryImplNode.setLeftNode(irLoadVariableNode);

            InvokeCallNode irInvokeCallNode = new InvokeCallNode(internalLocation);
            irInvokeCallNode.attachDecoration(new IRDExpressionType(Map.class));
            irInvokeCallNode.setBox(PainlessExplainError.class);
            irInvokeCallNode.setMethod(
                new PainlessMethod(
                    PainlessExplainError.class.getMethod("getHeaders", PainlessLookup.class),
                    PainlessExplainError.class,
                    null,
                    Collections.emptyList(),
                    null,
                    null,
                    null
                )
            );

            irBinaryImplNode.setRightNode(irInvokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(PainlessLookup.class));
            irLoadFieldMemberNode.attachDecoration(new IRDName("$DEFINITION"));
            irLoadFieldMemberNode.attachCondition(IRCStatic.class);

            irInvokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irThrowNode = createCatchAndThrow(SecurityException.class, internalLocation, irTryNode);
            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(SecurityException.class));
            irLoadVariableNode.attachDecoration(new IRDName(getExceptionVariableName(SecurityException.class)));
            irThrowNode.setExpressionNode(irLoadVariableNode);

            for (Class<? extends Throwable> throwable : List.of(
                PainlessError.class,
                LinkageError.class,
                OutOfMemoryError.class,
                StackOverflowError.class,
                Exception.class
            )) {

                irThrowNode = createCatchAndThrow(throwable, internalLocation, irTryNode);

                irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
                irInvokeCallMemberNode.attachDecoration(new IRDExpressionType(ScriptException.class));
                irInvokeCallMemberNode.attachDecoration(
                    new IRDFunction(
                        new LocalFunction(
                            "convertToScriptException",
                            ScriptException.class,
                            Arrays.asList(Throwable.class, Map.class),
                            true,
                            false
                        )
                    )
                );

                irThrowNode.setExpressionNode(irInvokeCallMemberNode);

                irLoadVariableNode = new LoadVariableNode(internalLocation);
                irLoadVariableNode.attachDecoration(new IRDExpressionType(ScriptException.class));
                irLoadVariableNode.attachDecoration(new IRDName(getExceptionVariableName(throwable)));

                irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

                irBinaryImplNode = new BinaryImplNode(internalLocation);
                irBinaryImplNode.attachDecoration(new IRDExpressionType(Map.class));

                irInvokeCallMemberNode.addArgumentNode(irBinaryImplNode);

                StaticNode irStaticNode = new StaticNode(internalLocation);
                irStaticNode.attachDecoration(new IRDExpressionType(Collections.class));

                irBinaryImplNode.setLeftNode(irStaticNode);

                irInvokeCallNode = new InvokeCallNode(internalLocation);
                irInvokeCallNode.attachDecoration(new IRDExpressionType(Map.class));
                irInvokeCallNode.setBox(Collections.class);
                irInvokeCallNode.setMethod(
                    new PainlessMethod(
                        Collections.class.getMethod("emptyMap"),
                        Collections.class,
                        null,
                        Collections.emptyList(),
                        null,
                        null,
                        null
                    )
                );

                irBinaryImplNode.setRightNode(irInvokeCallNode);
            }

            irBlockNode = new BlockNode(internalLocation);
            irBlockNode.attachCondition(IRCAllEscape.class);
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
        irCatchNode.attachDecoration(new IRDExceptionType(throwable));
        irCatchNode.attachDecoration(new IRDSymbol(name));

        irTryNode.addCatchNode(irCatchNode);

        BlockNode irCatchBlockNode = new BlockNode(internalLocation);
        irCatchBlockNode.attachCondition(IRCAllEscape.class);

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

    public void injectConverter(AStatement userStatementNode, ScriptScope scriptScope) {
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
        irInvokeCallMemberNode.attachDecoration(new IRDFunction(converter.converter()));
        ExpressionNode returnExpression = returnNode.getExpressionNode();
        returnNode.setExpressionNode(irInvokeCallMemberNode);
        irInvokeCallMemberNode.addArgumentNode(returnExpression);
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, ScriptScope scriptScope) {
        if ("$".equals(userCallLocalNode.getMethodName())) {
            PainlessMethod thisMethod = scriptScope.getDecoration(userCallLocalNode, ThisPainlessMethod.class).thisPainlessMethod();

            InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(userCallLocalNode.getLocation());
            irInvokeCallMemberNode.attachDecoration(new IRDThisMethod(thisMethod));
            irInvokeCallMemberNode.addArgumentNode(injectCast(userCallLocalNode.getArgumentNodes().get(0), scriptScope));
            irInvokeCallMemberNode.attachDecoration(new IRDExpressionType(def.class));

            InvokeCallDefNode irCallSubDefNode = new InvokeCallDefNode(userCallLocalNode.getLocation());
            irCallSubDefNode.addArgumentNode(injectCast(userCallLocalNode.getArgumentNodes().get(1), scriptScope));
            irCallSubDefNode.attachDecoration(new IRDExpressionType(def.class));
            irCallSubDefNode.attachDecoration(new IRDName("get"));

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(userCallLocalNode.getLocation());
            irBinaryImplNode.setLeftNode(irInvokeCallMemberNode);
            irBinaryImplNode.setRightNode(irCallSubDefNode);
            irBinaryImplNode.attachDecoration(new IRDExpressionType(def.class));

            scriptScope.putDecoration(userCallLocalNode, new IRNodeDecoration(irBinaryImplNode));
        } else {
            super.visitCallLocal(userCallLocalNode, scriptScope);
        }
    }
}
