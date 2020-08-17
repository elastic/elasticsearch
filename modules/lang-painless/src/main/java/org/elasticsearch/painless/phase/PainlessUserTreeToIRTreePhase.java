/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.PainlessError;
import org.elasticsearch.painless.PainlessExplainError;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.ScriptClassInfo.MethodArgument;
import org.elasticsearch.painless.ir.BinaryNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.FunctionNode;
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
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.symbol.Decorations.IRNodeDecoration;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
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
            LocalFunction localFunction =
                    scriptScope.getFunctionTable().getFunction(functionName, scriptClassInfo.getExecuteArguments().size());
            Class<?> returnType = localFunction.getReturnType();

            boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);
            BlockNode irBlockNode = (BlockNode)visit(userFunctionNode.getBlockNode(), scriptScope);

            if (methodEscape == false) {
                ExpressionNode irExpressionNode;

                if (returnType == void.class) {
                    irExpressionNode = null;
                } else {
                    if (returnType.isPrimitive()) {
                        ConstantNode irConstantNode = new ConstantNode();
                        irConstantNode.setLocation(userFunctionNode.getLocation());
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
                        irExpressionNode = new NullNode();
                        irExpressionNode.setLocation(userFunctionNode.getLocation());
                        irExpressionNode.setExpressionType(returnType);
                    }
                }

                ReturnNode irReturnNode = new ReturnNode();
                irReturnNode.setLocation(userFunctionNode.getLocation());
                irReturnNode.setExpressionNode(irExpressionNode);

                irBlockNode.addStatementNode(irReturnNode);
            }

            List<String> parameterNames = new ArrayList<>(scriptClassInfo.getExecuteArguments().size());

            for (MethodArgument methodArgument : scriptClassInfo.getExecuteArguments()) {
                parameterNames.add(methodArgument.getName());
            }

            FunctionNode irFunctionNode = new FunctionNode();
            irFunctionNode.setBlockNode(irBlockNode);
            irFunctionNode.setLocation(userFunctionNode.getLocation());
            irFunctionNode.setName("execute");
            irFunctionNode.setReturnType(returnType);
            irFunctionNode.getTypeParameters().addAll(localFunction.getTypeParameters());
            irFunctionNode.getParameterNames().addAll(parameterNames);
            irFunctionNode.setStatic(false);
            irFunctionNode.setVarArgs(false);
            irFunctionNode.setSynthetic(false);
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

        FieldNode irFieldNode = new FieldNode();
        irFieldNode.setLocation(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(String.class);
        irFieldNode.setName("$NAME");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode();
        irFieldNode.setLocation(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(String.class);
        irFieldNode.setName("$SOURCE");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode();
        irFieldNode.setLocation(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(BitSet.class);
        irFieldNode.setName("$STATEMENTS");

        irClassNode.addFieldNode(irFieldNode);

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setLocation(internalLocation);
        irFunctionNode.setName("getName");
        irFunctionNode.setReturnType(String.class);
        irFunctionNode.setStatic(false);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        BlockNode irBlockNode = new BlockNode();
        irBlockNode.setLocation(internalLocation);
        irBlockNode.setAllEscape(true);
        irBlockNode.setStatementCount(1);

        irFunctionNode.setBlockNode(irBlockNode);

        ReturnNode irReturnNode = new ReturnNode();
        irReturnNode.setLocation(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode();
        irLoadFieldMemberNode.setLocation(internalLocation);
        irLoadFieldMemberNode.setExpressionType(String.class);
        irLoadFieldMemberNode.setName("$NAME");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode();
        irFunctionNode.setLocation(internalLocation);
        irFunctionNode.setName("getSource");
        irFunctionNode.setReturnType(String.class);
        irFunctionNode.setStatic(false);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode();
        irBlockNode.setLocation(internalLocation);
        irBlockNode.setAllEscape(true);
        irBlockNode.setStatementCount(1);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode();
        irReturnNode.setLocation(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode();
        irLoadFieldMemberNode.setLocation(internalLocation);
        irLoadFieldMemberNode.setExpressionType(String.class);
        irLoadFieldMemberNode.setName("$SOURCE");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode();
        irFunctionNode.setLocation(internalLocation);
        irFunctionNode.setName("getStatements");
        irFunctionNode.setReturnType(BitSet.class);
        irFunctionNode.setStatic(false);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode();
        irBlockNode.setLocation(internalLocation);
        irBlockNode.setAllEscape(true);
        irBlockNode.setStatementCount(1);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode();
        irReturnNode.setLocation(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode();
        irLoadFieldMemberNode.setLocation(internalLocation);
        irLoadFieldMemberNode.setExpressionType(BitSet.class);
        irLoadFieldMemberNode.setName("$STATEMENTS");
        irLoadFieldMemberNode.setStatic(true);

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

                DeclarationNode irDeclarationNode = new DeclarationNode();
                irDeclarationNode.setLocation(internalLocation);
                irDeclarationNode.setName(name);
                irDeclarationNode.setDeclarationType(returnType);
                irBlockNode.getStatementsNodes().add(0, irDeclarationNode);

                InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode();
                irInvokeCallMemberNode.setLocation(internalLocation);
                irInvokeCallMemberNode.setExpressionType(irDeclarationNode.getDeclarationType());
                irInvokeCallMemberNode.setLocalFunction(new LocalFunction(
                        getMethod.getName(), returnType, Collections.emptyList(), true, false));
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

            FunctionNode irFunctionNode = new FunctionNode();
            irFunctionNode.setLocation(internalLocation);
            irFunctionNode.setName(needsMethod.getName());
            irFunctionNode.setReturnType(boolean.class);
            irFunctionNode.setStatic(false);
            irFunctionNode.setVarArgs(false);
            irFunctionNode.setSynthetic(true);
            irFunctionNode.setMaxLoopCounter(0);

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode irBlockNode = new BlockNode();
            irBlockNode.setLocation(internalLocation);
            irBlockNode.setAllEscape(true);
            irBlockNode.setStatementCount(1);

            irFunctionNode.setBlockNode(irBlockNode);

            ReturnNode irReturnNode = new ReturnNode();
            irReturnNode.setLocation(internalLocation);

            irBlockNode.addStatementNode(irReturnNode);

            ConstantNode irConstantNode = new ConstantNode();
            irConstantNode.setLocation(internalLocation);
            irConstantNode.setExpressionType(boolean.class);
            irConstantNode.setConstant(scriptScope.getUsedVariables().contains(name));

            irReturnNode.setExpressionNode(irConstantNode);
        }
    }

    // decorate the execute method with nodes to wrap the user statements with
    // the sandboxed errors as follows:
    // } catch (PainlessExplainError e) {
    //     throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
    // }
    // and
    // } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
    //     throw this.convertToScriptException(e, e.getHeaders())
    // }
    protected void injectSandboxExceptions(FunctionNode irFunctionNode) {
        try {
            Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectSandboxExceptions", 0);
            BlockNode irBlockNode = irFunctionNode.getBlockNode();

            TryNode irTryNode = new TryNode();
            irTryNode.setLocation(internalLocation);
            irTryNode.setBlockNode(irBlockNode);

            CatchNode irCatchNode = new CatchNode();
            irCatchNode.setLocation(internalLocation);
            irCatchNode.setExceptionType(PainlessExplainError.class);
            irCatchNode.setSymbol("#painlessExplainError");

            irTryNode.addCatchNode(irCatchNode);

            BlockNode irCatchBlockNode = new BlockNode();
            irCatchBlockNode.setLocation(internalLocation);
            irCatchBlockNode.setAllEscape(true);
            irCatchBlockNode.setStatementCount(1);

            irCatchNode.setBlockNode(irCatchBlockNode);

            ThrowNode irThrowNode = new ThrowNode();
            irThrowNode.setLocation(internalLocation);

            irCatchBlockNode.addStatementNode(irThrowNode);

            InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode();
            irInvokeCallMemberNode.setLocation(internalLocation);
            irInvokeCallMemberNode.setExpressionType(ScriptException.class);
            irInvokeCallMemberNode.setLocalFunction(
                    new LocalFunction(
                            "convertToScriptException",
                            ScriptException.class,
                            Arrays.asList(Throwable.class, Map.class),
                            true,
                            false
                    )
            );

            irThrowNode.setExpressionNode(irInvokeCallMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(ScriptException.class);
            irLoadVariableNode.setName("#painlessExplainError");

            irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

            BinaryNode irBinaryNode = new BinaryNode();
            irBinaryNode.setLocation(internalLocation);
            irBinaryNode.setExpressionType(Map.class);

            irInvokeCallMemberNode.addArgumentNode(irBinaryNode);

            irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(PainlessExplainError.class);
            irLoadVariableNode.setName("#painlessExplainError");

            irBinaryNode.setLeftNode(irLoadVariableNode);

            InvokeCallNode irInvokeCallNode = new InvokeCallNode();
            irInvokeCallNode.setLocation(internalLocation);
            irInvokeCallNode.setExpressionType(Map.class);
            irInvokeCallNode.setBox(PainlessExplainError.class);
            irInvokeCallNode.setMethod(
                    new PainlessMethod(
                            PainlessExplainError.class.getMethod(
                                    "getHeaders",
                                    PainlessLookup.class),
                            PainlessExplainError.class,
                            null,
                            Collections.emptyList(),
                            null,
                            null,
                            null
                    )
            );

            irBinaryNode.setRightNode(irInvokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode();
            irLoadFieldMemberNode.setLocation(internalLocation);
            irLoadFieldMemberNode.setExpressionType(PainlessLookup.class);
            irLoadFieldMemberNode.setName("$DEFINITION");
            irLoadFieldMemberNode.setStatic(true);

            irInvokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            for (Class<?> throwable : new Class<?>[] {
                    PainlessError.class, BootstrapMethodError.class, OutOfMemoryError.class, StackOverflowError.class, Exception.class}) {

                String name = throwable.getSimpleName();
                name = "#" + Character.toLowerCase(name.charAt(0)) + name.substring(1);

                irCatchNode = new CatchNode();
                irCatchNode.setLocation(internalLocation);
                irCatchNode.setExceptionType(throwable);
                irCatchNode.setSymbol(name);

                irTryNode.addCatchNode(irCatchNode);

                irCatchBlockNode = new BlockNode();
                irCatchBlockNode.setLocation(internalLocation);
                irCatchBlockNode.setAllEscape(true);
                irCatchBlockNode.setStatementCount(1);

                irCatchNode.setBlockNode(irCatchBlockNode);

                irThrowNode = new ThrowNode();
                irThrowNode.setLocation(internalLocation);

                irCatchBlockNode.addStatementNode(irThrowNode);

                irInvokeCallMemberNode = new InvokeCallMemberNode();
                irInvokeCallMemberNode.setLocation(internalLocation);
                irInvokeCallMemberNode.setExpressionType(ScriptException.class);
                irInvokeCallMemberNode.setLocalFunction(
                        new LocalFunction(
                                "convertToScriptException",
                                ScriptException.class,
                                Arrays.asList(Throwable.class, Map.class),
                                true,
                                false
                        )
                );

                irThrowNode.setExpressionNode(irInvokeCallMemberNode);

                irLoadVariableNode = new LoadVariableNode();
                irLoadVariableNode.setLocation(internalLocation);
                irLoadVariableNode.setExpressionType(ScriptException.class);
                irLoadVariableNode.setName(name);

                irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

                irBinaryNode = new BinaryNode();
                irBinaryNode.setLocation(internalLocation);
                irBinaryNode.setExpressionType(Map.class);

                irInvokeCallMemberNode.addArgumentNode(irBinaryNode);

                StaticNode irStaticNode = new StaticNode();
                irStaticNode.setLocation(internalLocation);
                irStaticNode.setExpressionType(Collections.class);

                irBinaryNode.setLeftNode(irStaticNode);

                irInvokeCallNode = new InvokeCallNode();
                irInvokeCallNode.setLocation(internalLocation);
                irInvokeCallNode.setExpressionType(Map.class);
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

                irBinaryNode.setRightNode(irInvokeCallNode);
            }

            irBlockNode = new BlockNode();
            irBlockNode.setLocation(irBlockNode.getLocation());
            irBlockNode.setAllEscape(irBlockNode.doAllEscape());
            irBlockNode.setStatementCount(irBlockNode.getStatementCount());
            irBlockNode.addStatementNode(irTryNode);

            irFunctionNode.setBlockNode(irBlockNode);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }
}
