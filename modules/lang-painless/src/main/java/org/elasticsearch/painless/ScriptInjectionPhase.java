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

import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.MemberCallNode;
import org.elasticsearch.painless.ir.MemberFieldLoadNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.script.ScriptException;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Method;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;

/**
 * This injects additional ir nodes required for
 * the "execute" method. This includes injection of ir nodes
 * to convert get methods into local variables for those
 * that are used and adds additional sandboxing by wrapping
 * the main "execute" block with several exceptions.
 */
public class ScriptInjectionPhase {

    public static void phase(ScriptScope scriptScope, ClassNode classNode) {
        FunctionNode executeFunctionNode = null;

        // look up the execute method for decoration
        for (FunctionNode functionNode : classNode.getFunctionsNodes()) {
            if ("execute".equals(functionNode.getName())) {
                executeFunctionNode = functionNode;
                break;
            }
        }

        if (executeFunctionNode == null) {
            throw new IllegalStateException("all scripts must have an [execute] method");
        }

        injectStaticFieldsAndGetters(classNode);
        injectGetsDeclarations(scriptScope, executeFunctionNode);
        injectNeedsMethods(scriptScope, classNode);
        injectSandboxExceptions(executeFunctionNode);
    }

    // adds static fields and getter methods required by PainlessScript for exception handling
    protected static void injectStaticFieldsAndGetters(ClassNode classNode) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectStaticFieldsAndGetters", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode fieldNode = new FieldNode();
        fieldNode.setLocation(internalLocation);
        fieldNode.setModifiers(modifiers);
        fieldNode.setFieldType(String.class);
        fieldNode.setName("$NAME");

        classNode.addFieldNode(fieldNode);

        fieldNode = new FieldNode();
        fieldNode.setLocation(internalLocation);
        fieldNode.setModifiers(modifiers);
        fieldNode.setFieldType(String.class);
        fieldNode.setName("$SOURCE");

        classNode.addFieldNode(fieldNode);

        fieldNode = new FieldNode();
        fieldNode.setLocation(internalLocation);
        fieldNode.setModifiers(modifiers);
        fieldNode.setFieldType(BitSet.class);
        fieldNode.setName("$STATEMENTS");

        classNode.addFieldNode(fieldNode);

        FunctionNode functionNode = new FunctionNode();
        functionNode.setLocation(internalLocation);
        functionNode.setName("getName");
        functionNode.setReturnType(String.class);
        functionNode.setStatic(false);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(true);
        functionNode.setMaxLoopCounter(0);

        classNode.addFunctionNode(functionNode);

        BlockNode blockNode = new BlockNode();
        blockNode.setLocation(internalLocation);
        blockNode.setAllEscape(true);
        blockNode.setStatementCount(1);

        functionNode.setBlockNode(blockNode);

        ReturnNode returnNode = new ReturnNode();
        returnNode.setLocation(internalLocation);

        blockNode.addStatementNode(returnNode);

        MemberFieldLoadNode memberFieldLoadNode = new MemberFieldLoadNode();
        memberFieldLoadNode.setLocation(internalLocation);
        memberFieldLoadNode.setExpressionType(String.class);
        memberFieldLoadNode.setName("$NAME");
        memberFieldLoadNode.setStatic(true);

        returnNode.setExpressionNode(memberFieldLoadNode);

        functionNode = new FunctionNode();
        functionNode.setLocation(internalLocation);
        functionNode.setName("getSource");
        functionNode.setReturnType(String.class);
        functionNode.setStatic(false);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(true);
        functionNode.setMaxLoopCounter(0);

        classNode.addFunctionNode(functionNode);

        blockNode = new BlockNode();
        blockNode.setLocation(internalLocation);
        blockNode.setAllEscape(true);
        blockNode.setStatementCount(1);

        functionNode.setBlockNode(blockNode);

        returnNode = new ReturnNode();
        returnNode.setLocation(internalLocation);

        blockNode.addStatementNode(returnNode);

        memberFieldLoadNode = new MemberFieldLoadNode();
        memberFieldLoadNode.setLocation(internalLocation);
        memberFieldLoadNode.setExpressionType(String.class);
        memberFieldLoadNode.setName("$SOURCE");
        memberFieldLoadNode.setStatic(true);

        returnNode.setExpressionNode(memberFieldLoadNode);

        functionNode = new FunctionNode();
        functionNode.setLocation(internalLocation);
        functionNode.setName("getStatements");
        functionNode.setReturnType(BitSet.class);
        functionNode.setStatic(false);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(true);
        functionNode.setMaxLoopCounter(0);

        classNode.addFunctionNode(functionNode);

        blockNode = new BlockNode();
        blockNode.setLocation(internalLocation);
        blockNode.setAllEscape(true);
        blockNode.setStatementCount(1);

        functionNode.setBlockNode(blockNode);

        returnNode = new ReturnNode();
        returnNode.setLocation(internalLocation);

        blockNode.addStatementNode(returnNode);

        memberFieldLoadNode = new MemberFieldLoadNode();
        memberFieldLoadNode.setLocation(internalLocation);
        memberFieldLoadNode.setExpressionType(BitSet.class);
        memberFieldLoadNode.setName("$STATEMENTS");
        memberFieldLoadNode.setStatic(true);

        returnNode.setExpressionNode(memberFieldLoadNode);
    }

    // convert gets methods to a new set of inserted ir nodes as necessary -
    // requires the gets method name be modified from "getExample" to "example"
    // if a get method variable isn't used it's declaration node is removed from
    // the ir tree permanently so there is no frivolous variable slotting
    protected static void injectGetsDeclarations(ScriptScope scriptScope, FunctionNode functionNode) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectGetsDeclarations", 0);
        BlockNode blockNode = functionNode.getBlockNode();
        int statementIndex = 0;

        while (statementIndex < blockNode.getStatementsNodes().size()) {
            StatementNode statementNode = blockNode.getStatementsNodes().get(statementIndex);

            if (statementNode instanceof DeclarationNode) {
                DeclarationNode declarationNode = (DeclarationNode)statementNode;
                boolean isRemoved = false;

                for (int getIndex = 0; getIndex < scriptScope.getScriptClassInfo().getGetMethods().size(); ++getIndex) {
                    Class<?> returnType = scriptScope.getScriptClassInfo().getGetReturns().get(getIndex);
                    Method getMethod = scriptScope.getScriptClassInfo().getGetMethods().get(getIndex);
                    String name = getMethod.getName().substring(3);
                    name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

                    if (name.equals(declarationNode.getName())) {
                        if (scriptScope.getUsedVariables().contains(name)) {
                            MemberCallNode memberCallNode = new MemberCallNode();
                            memberCallNode.setLocation(internalLocation);
                            memberCallNode.setExpressionType(declarationNode.getDeclarationType());
                            memberCallNode.setLocalFunction(new LocalFunction(
                                    getMethod.getName(), returnType, Collections.emptyList(), true, false));
                            declarationNode.setExpressionNode(memberCallNode);
                        } else {
                            blockNode.getStatementsNodes().remove(statementIndex);
                            isRemoved = true;
                        }

                        break;
                    }
                }

                if (isRemoved == false) {
                    ++statementIndex;
                }
            } else {
                ++statementIndex;
            }
        }
    }

    // injects needs methods as defined by ScriptClassInfo
    protected static void injectNeedsMethods(ScriptScope scriptScope, ClassNode classNode) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectNeedsMethods", 0);

        for (org.objectweb.asm.commons.Method needsMethod : scriptScope.getScriptClassInfo().getNeedsMethods()) {
            String name = needsMethod.getName();
            name = name.substring(5);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            FunctionNode functionNode = new FunctionNode();
            functionNode.setLocation(internalLocation);
            functionNode.setName(needsMethod.getName());
            functionNode.setReturnType(boolean.class);
            functionNode.setStatic(false);
            functionNode.setVarArgs(false);
            functionNode.setSynthetic(true);
            functionNode.setMaxLoopCounter(0);

            classNode.addFunctionNode(functionNode);

            BlockNode blockNode = new BlockNode();
            blockNode.setLocation(internalLocation);
            blockNode.setAllEscape(true);
            blockNode.setStatementCount(1);

            functionNode.setBlockNode(blockNode);

            ReturnNode returnNode = new ReturnNode();
            returnNode.setLocation(internalLocation);

            blockNode.addStatementNode(returnNode);

            ConstantNode constantNode = new ConstantNode();
            constantNode.setLocation(internalLocation);
            constantNode.setExpressionType(boolean.class);
            constantNode.setConstant(scriptScope.getUsedVariables().contains(name));

            returnNode.setExpressionNode(constantNode);
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
    protected static void injectSandboxExceptions(FunctionNode functionNode) {
        try {
            Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectSandboxExceptions", 0);
            BlockNode blockNode = functionNode.getBlockNode();

            TryNode tryNode = new TryNode();
            tryNode.setLocation(internalLocation);
            tryNode.setBlockNode(blockNode);

            CatchNode catchNode = new CatchNode();
            catchNode.setLocation(internalLocation);
            catchNode.setExceptionType(PainlessExplainError.class);
            catchNode.setSymbol("#painlessExplainError");

            tryNode.addCatchNode(catchNode);

            BlockNode catchBlockNode = new BlockNode();
            catchBlockNode.setLocation(internalLocation);
            catchBlockNode.setAllEscape(true);
            catchBlockNode.setStatementCount(1);

            catchNode.setBlockNode(catchBlockNode);

            ThrowNode throwNode = new ThrowNode();
            throwNode.setLocation(internalLocation);

            catchBlockNode.addStatementNode(throwNode);

            MemberCallNode memberCallNode = new MemberCallNode();
            memberCallNode.setLocation(internalLocation);
            memberCallNode.setExpressionType(ScriptException.class);
            memberCallNode.setLocalFunction(new LocalFunction(
                    "convertToScriptException",
                    ScriptException.class,
                    Arrays.asList(Throwable.class, Map.class),
                    true,
                    false
                    )
            );

            throwNode.setExpressionNode(memberCallNode);

            VariableNode variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(ScriptException.class);
            variableNode.setName("#painlessExplainError");

            memberCallNode.addArgumentNode(variableNode);

            CallNode callNode = new CallNode();
            callNode.setLocation(internalLocation);
            callNode.setExpressionType(Map.class);

            memberCallNode.addArgumentNode(callNode);

            variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(PainlessExplainError.class);
            variableNode.setName("#painlessExplainError");

            callNode.setLeftNode(variableNode);

            CallSubNode callSubNode = new CallSubNode();
            callSubNode.setLocation(internalLocation);
            callSubNode.setExpressionType(Map.class);
            callSubNode.setBox(PainlessExplainError.class);
            callSubNode.setMethod(new PainlessMethod(
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

            callNode.setRightNode(callSubNode);

            MemberFieldLoadNode memberFieldLoadNode = new MemberFieldLoadNode();
            memberFieldLoadNode.setLocation(internalLocation);
            memberFieldLoadNode.setExpressionType(PainlessLookup.class);
            memberFieldLoadNode.setName("$DEFINITION");
            memberFieldLoadNode.setStatic(true);

            callSubNode.addArgumentNode(memberFieldLoadNode);

            for (Class<?> throwable : new Class<?>[] {
                    PainlessError.class, BootstrapMethodError.class, OutOfMemoryError.class, StackOverflowError.class, Exception.class}) {

                String name = throwable.getSimpleName();
                name = "#" + Character.toLowerCase(name.charAt(0)) + name.substring(1);

                catchNode = new CatchNode();
                catchNode.setLocation(internalLocation);
                catchNode.setExceptionType(throwable);
                catchNode.setSymbol(name);

                tryNode.addCatchNode(catchNode);

                catchBlockNode = new BlockNode();
                catchBlockNode.setLocation(internalLocation);
                catchBlockNode.setAllEscape(true);
                catchBlockNode.setStatementCount(1);

                catchNode.setBlockNode(catchBlockNode);

                throwNode = new ThrowNode();
                throwNode.setLocation(internalLocation);

                catchBlockNode.addStatementNode(throwNode);

                memberCallNode = new MemberCallNode();
                memberCallNode.setLocation(internalLocation);
                memberCallNode.setExpressionType(ScriptException.class);
                memberCallNode.setLocalFunction(new LocalFunction(
                        "convertToScriptException",
                        ScriptException.class,
                        Arrays.asList(Throwable.class, Map.class),
                        true,
                        false
                        )
                );

                throwNode.setExpressionNode(memberCallNode);

                variableNode = new VariableNode();
                variableNode.setLocation(internalLocation);
                variableNode.setExpressionType(ScriptException.class);
                variableNode.setName(name);

                memberCallNode.addArgumentNode(variableNode);

                callNode = new CallNode();
                callNode.setLocation(internalLocation);
                callNode.setExpressionType(Map.class);

                memberCallNode.addArgumentNode(callNode);

                StaticNode staticNode = new StaticNode();
                staticNode.setLocation(internalLocation);
                staticNode.setExpressionType(Collections.class);

                callNode.setLeftNode(staticNode);

                callSubNode = new CallSubNode();
                callSubNode.setLocation(internalLocation);
                callSubNode.setExpressionType(Map.class);
                callSubNode.setBox(Collections.class);
                callSubNode.setMethod(new PainlessMethod(
                        Collections.class.getMethod("emptyMap"),
                        Collections.class,
                        null,
                        Collections.emptyList(),
                        null,
                        null,
                        null
                        )
                );

                callNode.setRightNode(callSubNode);
            }

            blockNode = new BlockNode();
            blockNode.setLocation(blockNode.getLocation());
            blockNode.setAllEscape(blockNode.doAllEscape());
            blockNode.setStatementCount(blockNode.getStatementCount());
            blockNode.addStatementNode(tryNode);

            functionNode.setBlockNode(blockNode);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private ScriptInjectionPhase() {
        // do nothing
    }
}
