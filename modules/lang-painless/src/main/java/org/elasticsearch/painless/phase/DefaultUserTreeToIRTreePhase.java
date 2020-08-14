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

import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BinaryNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConditionNode;
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
import org.elasticsearch.painless.ir.ReferenceNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.StoreAccessNode;
import org.elasticsearch.painless.ir.StoreBraceDefNode;
import org.elasticsearch.painless.ir.StoreBraceNode;
import org.elasticsearch.painless.ir.StoreDotDefNode;
import org.elasticsearch.painless.ir.StoreDotNode;
import org.elasticsearch.painless.ir.StoreDotShortcutNode;
import org.elasticsearch.painless.ir.StoreFieldMemberNode;
import org.elasticsearch.painless.ir.StoreListShortcutNode;
import org.elasticsearch.painless.ir.StoreMapShortcutNode;
import org.elasticsearch.painless.ir.StoreNode;
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
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.EAssignment;
import org.elasticsearch.painless.node.EBinary;
import org.elasticsearch.painless.node.EBooleanComp;
import org.elasticsearch.painless.node.EBooleanConstant;
import org.elasticsearch.painless.node.EBrace;
import org.elasticsearch.painless.node.ECall;
import org.elasticsearch.painless.node.ECallLocal;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.node.EConditional;
import org.elasticsearch.painless.node.EDecimal;
import org.elasticsearch.painless.node.EDot;
import org.elasticsearch.painless.node.EElvis;
import org.elasticsearch.painless.node.EExplicit;
import org.elasticsearch.painless.node.EFunctionRef;
import org.elasticsearch.painless.node.EInstanceof;
import org.elasticsearch.painless.node.ELambda;
import org.elasticsearch.painless.node.EListInit;
import org.elasticsearch.painless.node.EMapInit;
import org.elasticsearch.painless.node.ENewArray;
import org.elasticsearch.painless.node.ENewArrayFunctionRef;
import org.elasticsearch.painless.node.ENewObj;
import org.elasticsearch.painless.node.ENull;
import org.elasticsearch.painless.node.ENumeric;
import org.elasticsearch.painless.node.ERegex;
import org.elasticsearch.painless.node.EString;
import org.elasticsearch.painless.node.ESymbol;
import org.elasticsearch.painless.node.EUnary;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SBreak;
import org.elasticsearch.painless.node.SCatch;
import org.elasticsearch.painless.node.SClass;
import org.elasticsearch.painless.node.SContinue;
import org.elasticsearch.painless.node.SDeclBlock;
import org.elasticsearch.painless.node.SDeclaration;
import org.elasticsearch.painless.node.SDo;
import org.elasticsearch.painless.node.SEach;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFor;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.node.SIf;
import org.elasticsearch.painless.node.SIfElse;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.node.SThrow;
import org.elasticsearch.painless.node.STry;
import org.elasticsearch.painless.node.SWhile;
import org.elasticsearch.painless.symbol.Decorations.AccessDepth;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.BinaryType;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.ComparisonType;
import org.elasticsearch.painless.symbol.Decorations.Compound;
import org.elasticsearch.painless.symbol.Decorations.CompoundType;
import org.elasticsearch.painless.symbol.Decorations.Concatenate;
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.DowncastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.GetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.IRNodeDecoration;
import org.elasticsearch.painless.symbol.Decorations.InstanceType;
import org.elasticsearch.painless.symbol.Decorations.IterablePainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.ListShortcut;
import org.elasticsearch.painless.symbol.Decorations.MapShortcut;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodNameDecoration;
import org.elasticsearch.painless.symbol.Decorations.Negate;
import org.elasticsearch.painless.symbol.Decorations.ParameterNames;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ReferenceDecoration;
import org.elasticsearch.painless.symbol.Decorations.ReturnType;
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.Decorations.SetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.ShiftType;
import org.elasticsearch.painless.symbol.Decorations.Shortcut;
import org.elasticsearch.painless.symbol.Decorations.StandardConstant;
import org.elasticsearch.painless.symbol.Decorations.StandardLocalFunction;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessClassBinding;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessConstructor;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessField;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessInstanceBinding;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.TypeParameters;
import org.elasticsearch.painless.symbol.Decorations.UnaryType;
import org.elasticsearch.painless.symbol.Decorations.UpcastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class DefaultUserTreeToIRTreePhase implements UserTreeVisitor<ScriptScope> {

    protected ClassNode irClassNode;

    /**
     * This injects additional ir nodes required for resolving the def type at runtime.
     * This includes injection of ir nodes to add a function to call
     * {@link DefBootstrap#bootstrap(PainlessLookup, FunctionTable, Lookup, String, MethodType, int, int, Object...)}
     * to do the runtime resolution, and several supporting static fields.
     */
    protected void injectBootstrapMethod(ScriptScope scriptScope) {
        // adds static fields required for def bootstrapping
        Location internalLocation = new Location("$internal$injectStaticFields", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode irFieldNode = new FieldNode();
        irFieldNode.setLocation(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(PainlessLookup.class);
        irFieldNode.setName("$DEFINITION");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode();
        irFieldNode.setLocation(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(FunctionTable.class);
        irFieldNode.setName("$FUNCTIONS");

        irClassNode.addFieldNode(irFieldNode);

        // adds the bootstrap method required for dynamic binding for def type resolution
        internalLocation = new Location("$internal$injectDefBootstrapMethod", 0);

        try {
            FunctionNode irFunctionNode = new FunctionNode();
            irFunctionNode.setLocation(internalLocation);
            irFunctionNode.setReturnType(CallSite.class);
            irFunctionNode.setName("$bootstrapDef");
            irFunctionNode.getTypeParameters().addAll(
                    Arrays.asList(Lookup.class, String.class, MethodType.class, int.class, int.class, Object[].class));
            irFunctionNode.getParameterNames().addAll(
                    Arrays.asList("methodHandlesLookup", "name", "type", "initialDepth", "flavor", "args"));
            irFunctionNode.setStatic(true);
            irFunctionNode.setVarArgs(true);
            irFunctionNode.setSynthetic(true);
            irFunctionNode.setMaxLoopCounter(0);

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode blockNode = new BlockNode();
            blockNode.setLocation(internalLocation);
            blockNode.setAllEscape(true);
            blockNode.setStatementCount(1);

            irFunctionNode.setBlockNode(blockNode);

            ReturnNode returnNode = new ReturnNode();
            returnNode.setLocation(internalLocation);

            blockNode.addStatementNode(returnNode);

            BinaryNode irBinaryNode = new BinaryNode();
            irBinaryNode.setLocation(internalLocation);
            irBinaryNode.setExpressionType(CallSite.class);

            returnNode.setExpressionNode(irBinaryNode);

            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(internalLocation);
            staticNode.setExpressionType(DefBootstrap.class);

            irBinaryNode.setLeftNode(staticNode);

            InvokeCallNode invokeCallNode = new InvokeCallNode();
            invokeCallNode.setLocation(internalLocation);
            invokeCallNode.setExpressionType(CallSite.class);
            invokeCallNode.setMethod(new PainlessMethod(
                            DefBootstrap.class.getMethod("bootstrap",
                                    PainlessLookup.class,
                                    FunctionTable.class,
                                    Lookup.class,
                                    String.class,
                                    MethodType.class,
                                    int.class,
                                    int.class,
                                    Object[].class),
                            DefBootstrap.class,
                            CallSite.class,
                            Arrays.asList(
                                    PainlessLookup.class,
                                    FunctionTable.class,
                                    Lookup.class,
                                    String.class,
                                    MethodType.class,
                                    int.class,
                                    int.class,
                                    Object[].class),
                            null,
                            null,
                            null
                    )
            );
            invokeCallNode.setBox(DefBootstrap.class);

            irBinaryNode.setRightNode(invokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode();
            irLoadFieldMemberNode.setLocation(internalLocation);
            irLoadFieldMemberNode.setExpressionType(PainlessLookup.class);
            irLoadFieldMemberNode.setName("$DEFINITION");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode();
            irLoadFieldMemberNode.setLocation(internalLocation);
            irLoadFieldMemberNode.setExpressionType(FunctionTable.class);
            irLoadFieldMemberNode.setName("$FUNCTIONS");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(Lookup.class);
            irLoadVariableNode.setName("methodHandlesLookup");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(String.class);
            irLoadVariableNode.setName("name");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(MethodType.class);
            irLoadVariableNode.setName("type");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(int.class);
            irLoadVariableNode.setName("initialDepth");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(int.class);
            irLoadVariableNode.setName("flavor");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode();
            irLoadVariableNode.setLocation(internalLocation);
            irLoadVariableNode.setExpressionType(Object[].class);
            irLoadVariableNode.setName("args");

            invokeCallNode.addArgumentNode(irLoadVariableNode);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    protected ExpressionNode injectCast(AExpression userExpressionNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode = (ExpressionNode)visit(userExpressionNode, scriptScope);

        if (irExpressionNode == null) {
            return null;
        }

        ExpressionPainlessCast expressionPainlessCast = scriptScope.getDecoration(userExpressionNode, ExpressionPainlessCast.class);

        if (expressionPainlessCast == null) {
            return irExpressionNode;
        }

        CastNode irCastNode = new CastNode();
        irCastNode.setLocation(irExpressionNode.getLocation());
        irCastNode.setExpressionType(expressionPainlessCast.getExpressionPainlessCast().targetType);
        irCastNode.setCast(expressionPainlessCast.getExpressionPainlessCast());
        irCastNode.setChildNode(irExpressionNode);

        return irCastNode;
    }

    /**
     * This helper generates a set of ir nodes that are required for an assignment
     * handling both regular assignment and compound assignment. It only stubs out
     * the compound assignment.
     * @param accessDepth The number of arguments to dup for an additional read.
     * @param location The location for errors.
     * @param isNullSafe Whether or not the null safe operator is used.
     * @param irPrefixNode The prefix node for this store/load. The 'a.b' of 'a.b.c', etc.
     * @param irIndexNode The index node if this is a brace access.
     * @param irLoadNode The load node if this a read.
     * @param irStoreNode The store node if this is a write.
     * @return The root node for this assignment.
     */
    protected ExpressionNode buildLoadStore(int accessDepth, Location location, boolean isNullSafe,
            ExpressionNode irPrefixNode, ExpressionNode irIndexNode, ExpressionNode irLoadNode, StoreNode irStoreNode) {

        ExpressionNode irExpressionNode;

        // this load/store is only a load (read)
        if (irStoreNode == null) {
            ExpressionNode irAccessNode;

            // this load is a symbol or dot load with no index node
            if (irIndexNode == null) {
                irAccessNode = irLoadNode;
            // this load is a dot or brace load with an index node
            } else {
                BinaryNode irBinaryNode = new BinaryNode();
                irBinaryNode.setLocation(location);
                irBinaryNode.setExpressionType(irLoadNode.getExpressionType());
                irBinaryNode.setLeftNode(irIndexNode);
                irBinaryNode.setRightNode(irLoadNode);

                irAccessNode = irBinaryNode;
            }

            // this wraps the load if this is a null-safe operation
            if (isNullSafe) {
                NullSafeSubNode nullSafeSubNode = new NullSafeSubNode();
                nullSafeSubNode.setChildNode(irAccessNode);
                nullSafeSubNode.setLocation(location);
                nullSafeSubNode.setExpressionType(irAccessNode.getExpressionType());
                irAccessNode = nullSafeSubNode;
            }

            // this load is a symbol access with no prefix
            if (irPrefixNode == null) {
                irExpressionNode = irAccessNode;
            // this load is a dot or brace access with a prefix node
            } else {
                BinaryNode irParentNode = new BinaryNode();
                irParentNode.setLocation(location);
                irParentNode.setExpressionType(irLoadNode.getExpressionType());
                irParentNode.setLeftNode(irPrefixNode);
                irParentNode.setRightNode(irAccessNode);

                irExpressionNode = irParentNode;
            }
        // this is a store (write) and possibly also a load (read) for compound assignment
        } else {
            ExpressionNode irAccessNode;

            // this store is a symbol or dot store with no index node
            if (irIndexNode == null) {
                irAccessNode = irPrefixNode;
            // this store is a dot or brace load with an index node
            } else {
                BinaryNode irBinaryNode = new BinaryNode();
                irBinaryNode.setLocation(location);
                irBinaryNode.setExpressionType(void.class);
                irBinaryNode.setLeftNode(irPrefixNode);
                irBinaryNode.setRightNode(irIndexNode);

                irAccessNode = irBinaryNode;
            }

            // this is a simple store
            if (irLoadNode == null) {
                // this store is a dot or brace store
                if (irAccessNode != null) {
                    ((StoreAccessNode)irStoreNode).setAccessNode(irAccessNode);
                }
            // this is a compound assignment
            } else {
                // this store has a prefix node that we must dup for a load
                if (irAccessNode != null) {
                    DupNode dupNode = new DupNode();
                    dupNode.setLocation(location);
                    dupNode.setExpressionType(void.class);
                    dupNode.setSize(accessDepth);
                    dupNode.setDepth(0);
                    dupNode.setChildNode(irAccessNode);

                    ((StoreAccessNode)irStoreNode).setAccessNode(dupNode);
                }

                irStoreNode.setChildNode(irLoadNode);
            }

            irExpressionNode = irStoreNode;
        }

        return irExpressionNode;
    }

    protected IRNode visit(ANode userNode, ScriptScope scriptScope) {
        if (userNode == null) {
            return null;
        } else {
            userNode.visit(this, scriptScope);
            return scriptScope.getDecoration(userNode, IRNodeDecoration.class).getIRNode();
        }
    }

    @Override
    public void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        irClassNode = new ClassNode();

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode)visit(userFunctionNode, scriptScope));
        }

        irClassNode.setLocation(irClassNode.getLocation());
        irClassNode.setScriptScope(scriptScope);

        injectBootstrapMethod(scriptScope);
        scriptScope.putDecoration(userClassNode, new IRNodeDecoration(irClassNode));
    }

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();
        int functionArity = userFunctionNode.getCanonicalTypeNameParameters().size();
        LocalFunction localFunction = scriptScope.getFunctionTable().getFunction(functionName, functionArity);
        Class<?> returnType = localFunction.getReturnType();
        boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);

        BlockNode irBlockNode = (BlockNode)visit(userFunctionNode.getBlockNode(), scriptScope);

        if (methodEscape == false) {
            ExpressionNode irExpressionNode;

            if (returnType == void.class) {
                irExpressionNode = null;
            } else if (userFunctionNode.isAutoReturnEnabled()) {
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
            } else {
                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
            }

            ReturnNode irReturnNode = new ReturnNode();
            irReturnNode.setLocation(userFunctionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);

            irBlockNode.addStatementNode(irReturnNode);
        }

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setBlockNode(irBlockNode);
        irFunctionNode.setLocation(userFunctionNode.getLocation());
        irFunctionNode.setName(userFunctionNode.getFunctionName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.getTypeParameters().addAll(localFunction.getTypeParameters());
        irFunctionNode.getParameterNames().addAll(userFunctionNode.getParameterNames());
        irFunctionNode.setStatic(userFunctionNode.isStatic());
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(userFunctionNode.isSynthetic());
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());

        scriptScope.putDecoration(userFunctionNode, new IRNodeDecoration(irFunctionNode));
    }

    @Override
    public void visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        BlockNode irBlockNode = new BlockNode();

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode)visit(userStatementNode, scriptScope));
        }

        irBlockNode.setLocation(userBlockNode.getLocation());
        irBlockNode.setAllEscape(scriptScope.getCondition(userBlockNode, AllEscape.class));

        scriptScope.putDecoration(userBlockNode, new IRNodeDecoration(irBlockNode));
    }

    @Override
    public void visitIf(SIf userIfNode, ScriptScope scriptScope) {
        IfNode irIfNode = new IfNode();
        irIfNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfNode.setBlockNode((BlockNode)visit(userIfNode.getIfBlockNode(), scriptScope));
        irIfNode.setLocation(userIfNode.getLocation());

        scriptScope.putDecoration(userIfNode, new IRNodeDecoration(irIfNode));
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, ScriptScope scriptScope) {
        IfElseNode irIfElseNode = new IfElseNode();
        irIfElseNode.setConditionNode(injectCast(userIfElseNode.getConditionNode(), scriptScope));
        irIfElseNode.setBlockNode((BlockNode)visit(userIfElseNode.getIfBlockNode(), scriptScope));
        irIfElseNode.setElseBlockNode((BlockNode)visit(userIfElseNode.getElseBlockNode(), scriptScope));
        irIfElseNode.setLocation(userIfElseNode.getLocation());

        scriptScope.putDecoration(userIfElseNode, new IRNodeDecoration(irIfElseNode));
    }

    @Override
    public void visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        WhileLoopNode irWhileLoopNode = new WhileLoopNode();
        irWhileLoopNode.setConditionNode(injectCast(userWhileNode.getConditionNode(), scriptScope));
        irWhileLoopNode.setBlockNode((BlockNode)visit(userWhileNode.getBlockNode(), scriptScope));
        irWhileLoopNode.setLocation(userWhileNode.getLocation());
        irWhileLoopNode.setContinuous(scriptScope.getCondition(userWhileNode, ContinuousLoop.class));

        scriptScope.putDecoration(userWhileNode, new IRNodeDecoration(irWhileLoopNode));
    }

    @Override
    public void visitDo(SDo userDoNode, ScriptScope scriptScope) {
        DoWhileLoopNode irDoWhileLoopNode = new DoWhileLoopNode();
        irDoWhileLoopNode.setConditionNode(injectCast(userDoNode.getConditionNode(), scriptScope));
        irDoWhileLoopNode.setBlockNode((BlockNode)visit(userDoNode.getBlockNode(), scriptScope));
        irDoWhileLoopNode.setLocation(userDoNode.getLocation());
        irDoWhileLoopNode.setContinuous(scriptScope.getCondition(userDoNode, ContinuousLoop.class));

        scriptScope.putDecoration(userDoNode, new IRNodeDecoration(irDoWhileLoopNode));
    }

    @Override
    public void visitFor(SFor userForNode, ScriptScope scriptScope) {
        ForLoopNode irForLoopNode = new ForLoopNode();
        irForLoopNode.setInitialzerNode(visit(userForNode.getInitializerNode(), scriptScope));
        irForLoopNode.setConditionNode(injectCast(userForNode.getConditionNode(), scriptScope));
        irForLoopNode.setAfterthoughtNode((ExpressionNode)visit(userForNode.getAfterthoughtNode(), scriptScope));
        irForLoopNode.setBlockNode((BlockNode)visit(userForNode.getBlockNode(), scriptScope));
        irForLoopNode.setLocation(userForNode.getLocation());
        irForLoopNode.setContinuous(scriptScope.getCondition(userForNode, ContinuousLoop.class));

        scriptScope.putDecoration(userForNode, new IRNodeDecoration(irForLoopNode));
    }

    @Override
    public void visitEach(SEach userEachNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userEachNode, SemanticVariable.class).getSemanticVariable();
        PainlessCast painlessCast = scriptScope.hasDecoration(userEachNode, ExpressionPainlessCast.class) ?
                scriptScope.getDecoration(userEachNode, ExpressionPainlessCast.class).getExpressionPainlessCast() : null;
        ExpressionNode irIterableNode = (ExpressionNode)visit(userEachNode.getIterableNode(), scriptScope);
        Class<?> iterableValueType = scriptScope.getDecoration(userEachNode.getIterableNode(), ValueType.class).getValueType();
        BlockNode irBlockNode = (BlockNode)visit(userEachNode.getBlockNode(), scriptScope);

        ConditionNode irConditionNode;

        if (iterableValueType.isArray()) {
            ForEachSubArrayNode irForEachSubArrayNode = new ForEachSubArrayNode();
            irForEachSubArrayNode.setConditionNode(irIterableNode);
            irForEachSubArrayNode.setBlockNode(irBlockNode);
            irForEachSubArrayNode.setLocation(userEachNode.getLocation());
            irForEachSubArrayNode.setVariableType(variable.getType());
            irForEachSubArrayNode.setVariableName(variable.getName());
            irForEachSubArrayNode.setCast(painlessCast);
            irForEachSubArrayNode.setArrayType(iterableValueType);
            irForEachSubArrayNode.setArrayName("#array" + userEachNode.getLocation().getOffset());
            irForEachSubArrayNode.setIndexType(int.class);
            irForEachSubArrayNode.setIndexName("#index" + userEachNode.getLocation().getOffset());
            irForEachSubArrayNode.setIndexedType(iterableValueType.getComponentType());
            irForEachSubArrayNode.setContinuous(false);
            irConditionNode = irForEachSubArrayNode;
        } else if (iterableValueType == def.class || Iterable.class.isAssignableFrom(iterableValueType)) {
            ForEachSubIterableNode irForEachSubIterableNode = new ForEachSubIterableNode();
            irForEachSubIterableNode.setConditionNode(irIterableNode);
            irForEachSubIterableNode.setBlockNode(irBlockNode);
            irForEachSubIterableNode.setLocation(userEachNode.getLocation());
            irForEachSubIterableNode.setVariableType(variable.getType());
            irForEachSubIterableNode.setVariableName(variable.getName());
            irForEachSubIterableNode.setCast(painlessCast);
            irForEachSubIterableNode.setIteratorType(Iterator.class);
            irForEachSubIterableNode.setIteratorName("#itr" + userEachNode.getLocation().getOffset());
            irForEachSubIterableNode.setMethod(iterableValueType == def.class ? null :
                    scriptScope.getDecoration(userEachNode, IterablePainlessMethod.class).getIterablePainlessMethod());
            irForEachSubIterableNode.setContinuous(false);
            irConditionNode = irForEachSubIterableNode;
        } else {
            throw userEachNode.createError(new IllegalStateException("illegal tree structure"));
        }

        ForEachLoopNode irForEachLoopNode = new ForEachLoopNode();
        irForEachLoopNode.setConditionNode(irConditionNode);
        irForEachLoopNode.setLocation(userEachNode.getLocation());

        scriptScope.putDecoration(userEachNode, new IRNodeDecoration(irForEachLoopNode));
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scriptScope) {
        DeclarationBlockNode irDeclarationBlockNode = new DeclarationBlockNode();

        for (SDeclaration userDeclarationNode : userDeclBlockNode.getDeclarationNodes()) {
            irDeclarationBlockNode.addDeclarationNode((DeclarationNode)visit(userDeclarationNode, scriptScope));
        }

        irDeclarationBlockNode.setLocation(userDeclBlockNode.getLocation());

        scriptScope.putDecoration(userDeclBlockNode, new IRNodeDecoration(irDeclarationBlockNode));
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userDeclarationNode, SemanticVariable.class).getSemanticVariable();

        DeclarationNode irDeclarationNode = new DeclarationNode();
        irDeclarationNode.setExpressionNode(injectCast(userDeclarationNode.getValueNode(), scriptScope));
        irDeclarationNode.setLocation(userDeclarationNode.getLocation());
        irDeclarationNode.setDeclarationType(variable.getType());
        irDeclarationNode.setName(variable.getName());

        scriptScope.putDecoration(userDeclarationNode, new IRNodeDecoration(irDeclarationNode));
    }

    @Override
    public void visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        ReturnNode irReturnNode = new ReturnNode();
        irReturnNode.setExpressionNode(injectCast(userReturnNode.getValueNode(), scriptScope));
        irReturnNode.setLocation(userReturnNode.getLocation());

        scriptScope.putDecoration(userReturnNode, new IRNodeDecoration(irReturnNode));
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        StatementNode irStatementNode;
        ExpressionNode irExpressionNode = injectCast(userExpressionNode.getStatementNode(), scriptScope);

        if (scriptScope.getCondition(userExpressionNode, MethodEscape.class)) {
            ReturnNode irReturnNode = new ReturnNode();
            irReturnNode.setExpressionNode(irExpressionNode);
            irReturnNode.setLocation(userExpressionNode.getLocation());
            irStatementNode = irReturnNode;
        } else {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode();
            irStatementExpressionNode.setExpressionNode(irExpressionNode);
            irStatementExpressionNode.setLocation(userExpressionNode.getLocation());
            irStatementNode = irStatementExpressionNode;
        }

        scriptScope.putDecoration(userExpressionNode, new IRNodeDecoration(irStatementNode));
    }

    @Override
    public void visitTry(STry userTryNode, ScriptScope scriptScope) {
        TryNode irTryNode = new TryNode();

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            irTryNode.addCatchNode((CatchNode)visit(userCatchNode, scriptScope));
        }

        irTryNode.setBlockNode((BlockNode)visit(userTryNode.getBlockNode(), scriptScope));
        irTryNode.setLocation(userTryNode.getLocation());

        scriptScope.putDecoration(userTryNode, new IRNodeDecoration(irTryNode));
    }

    @Override
    public void visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userCatchNode, SemanticVariable.class).getSemanticVariable();

        CatchNode irCatchNode = new CatchNode();
        irCatchNode.setExceptionType(variable.getType());
        irCatchNode.setSymbol(variable.getName());
        irCatchNode.setBlockNode((BlockNode)visit(userCatchNode.getBlockNode(), scriptScope));
        irCatchNode.setLocation(userCatchNode.getLocation());

        scriptScope.putDecoration(userCatchNode, new IRNodeDecoration(irCatchNode));
    }

    @Override
    public void visitThrow(SThrow userThrowNode, ScriptScope scriptScope) {
        ThrowNode irThrowNode = new ThrowNode();
        irThrowNode.setExpressionNode(injectCast(userThrowNode.getExpressionNode(), scriptScope));
        irThrowNode.setLocation(userThrowNode.getLocation());

        scriptScope.putDecoration(userThrowNode, new IRNodeDecoration(irThrowNode));
    }

    @Override
    public void visitContinue(SContinue userContinueNode, ScriptScope scriptScope) {
        ContinueNode irContinueNode = new ContinueNode();
        irContinueNode.setLocation(userContinueNode.getLocation());

        scriptScope.putDecoration(userContinueNode, new IRNodeDecoration(irContinueNode));
    }

    @Override
    public void visitBreak(SBreak userBreakNode, ScriptScope scriptScope) {
        BreakNode irBreakNode = new BreakNode();
        irBreakNode.setLocation(userBreakNode.getLocation());

        scriptScope.putDecoration(userBreakNode, new IRNodeDecoration(irBreakNode));
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, ScriptScope scriptScope) {
        boolean read = scriptScope.getCondition(userAssignmentNode, Read.class);
        Class<?> compoundType = scriptScope.hasDecoration(userAssignmentNode, CompoundType.class) ?
                scriptScope.getDecoration(userAssignmentNode, CompoundType.class).getCompoundType() : null;

        StoreNode irStoreNode;
        // add a cast node if necessary for the value node for the assignment
        ExpressionNode irValueNode = injectCast(userAssignmentNode.getRightNode(), scriptScope);

        // handles a compound assignment using the stub generated from buildLoadStore
        if (compoundType != null) {
            boolean concatenate = userAssignmentNode.getOperation() == Operation.ADD && compoundType == String.class;
            scriptScope.setCondition(userAssignmentNode.getLeftNode(), Compound.class);
            irStoreNode = (StoreNode)visit(userAssignmentNode.getLeftNode(), scriptScope);
            ExpressionNode irLoadNode = irStoreNode.getChildNode();
            ExpressionNode irCompoundNode;

            // handles when the operation is a string concatenation
            if (concatenate) {
                StringConcatenationNode stringConcatenationNode = new StringConcatenationNode();
                stringConcatenationNode.setLocation(irStoreNode.getLocation());
                stringConcatenationNode.setExpressionType(String.class);
                stringConcatenationNode.setCat(false);
                irCompoundNode = stringConcatenationNode;
            // handles when the operation is mathematical
            } else {
                BinaryMathNode irBinaryMathNode = new BinaryMathNode();
                irBinaryMathNode.setLocation(irStoreNode.getLocation());
                irBinaryMathNode.setLeftNode(irLoadNode);
                irBinaryMathNode.setExpressionType(compoundType);
                irBinaryMathNode.setBinaryType(compoundType);
                irBinaryMathNode.setOperation(userAssignmentNode.getOperation());
                // add a compound assignment flag to the binary math node
                irBinaryMathNode.setFlags(DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT);
                irCompoundNode = irBinaryMathNode;
            }

            PainlessCast downcast = scriptScope.hasDecoration(userAssignmentNode, DowncastPainlessCast.class) ?
                    scriptScope.getDecoration(userAssignmentNode, DowncastPainlessCast.class).getDowncastPainlessCast() : null;

            // no need to downcast so the binary math node is the value for the store node
            if (downcast == null) {
                irCompoundNode.setExpressionType(irStoreNode.getStoreType());
                irStoreNode.setChildNode(irCompoundNode);
            // add a cast node to do a downcast as the value for the store node
            } else {
                CastNode irCastNode = new CastNode();
                irCastNode.setLocation(irCompoundNode.getLocation());
                irCastNode.setExpressionType(downcast.targetType);
                irCastNode.setCast(downcast);
                irCastNode.setChildNode(irCompoundNode);
                irStoreNode.setChildNode(irCastNode);
            }

            // the value is also read from this assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).getAccessDepth();
                DupNode irDupNode = new DupNode();

                // the value is read from prior to assignment (post-increment)
                if (userAssignmentNode.postIfRead()) {
                    irDupNode.setLocation(irLoadNode.getLocation());
                    irDupNode.setExpressionType(irLoadNode.getExpressionType());
                    irDupNode.setSize(MethodWriter.getType(irLoadNode.getExpressionType()).getSize());
                    irDupNode.setDepth(accessDepth);
                    irDupNode.setChildNode(irLoadNode);
                    irLoadNode = irDupNode;
                // the value is read from after the assignment (pre-increment/compound)
                } else {
                    irDupNode.setLocation(irStoreNode.getLocation());
                    irDupNode.setExpressionType(irStoreNode.getStoreType());
                    irDupNode.setSize(MethodWriter.getType(irStoreNode.getExpressionType()).getSize());
                    irDupNode.setDepth(accessDepth);
                    irDupNode.setChildNode(irStoreNode.getChildNode());
                    irStoreNode.setChildNode(irDupNode);
                }
            }

            PainlessCast upcast = scriptScope.hasDecoration(userAssignmentNode, UpcastPainlessCast.class) ?
                    scriptScope.getDecoration(userAssignmentNode, UpcastPainlessCast.class).getUpcastPainlessCast() : null;

            // upcast the stored value if necessary
            if (upcast != null) {
                CastNode irCastNode = new CastNode();
                irCastNode.setLocation(irLoadNode.getLocation());
                irCastNode.setExpressionType(upcast.targetType);
                irCastNode.setCast(upcast);
                irCastNode.setChildNode(irLoadNode);
                irLoadNode = irCastNode;
            }

            if (concatenate) {
                StringConcatenationNode irStringConcatenationNode = (StringConcatenationNode)irCompoundNode;
                irStringConcatenationNode.addArgumentNode(irLoadNode);
                irStringConcatenationNode.addArgumentNode(irValueNode);
            } else {
                BinaryMathNode irBinaryMathNode = (BinaryMathNode)irCompoundNode;
                irBinaryMathNode.setLeftNode(irLoadNode);
                irBinaryMathNode.setRightNode(irValueNode);
            }
        // handles a standard assignment
        } else {
            irStoreNode = (StoreNode)visit(userAssignmentNode.getLeftNode(), scriptScope);

            // the value is read from after the assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).getAccessDepth();

                DupNode irDupNode = new DupNode();
                irDupNode.setLocation(irValueNode.getLocation());
                irDupNode.setExpressionType(irValueNode.getExpressionType());
                irDupNode.setSize(MethodWriter.getType(irValueNode.getExpressionType()).getSize());
                irDupNode.setDepth(accessDepth);
                irDupNode.setChildNode(irValueNode);

                irValueNode = irDupNode;
            }

            irStoreNode.setChildNode(irValueNode);
        }

        scriptScope.putDecoration(userAssignmentNode, new IRNodeDecoration(irStoreNode));
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, ScriptScope scriptScope) {
        Class<?> unaryType = scriptScope.hasDecoration(userUnaryNode, UnaryType.class) ?
                scriptScope.getDecoration(userUnaryNode, UnaryType.class).getUnaryType() : null;

        IRNode irNode;

        if (scriptScope.getCondition(userUnaryNode.getChildNode(), Negate.class)) {
            irNode = visit(userUnaryNode.getChildNode(), scriptScope);
        } else {
            UnaryMathNode irUnaryMathNode = new UnaryMathNode();
            irUnaryMathNode.setLocation(userUnaryNode.getLocation());
            irUnaryMathNode.setExpressionType(scriptScope.getDecoration(userUnaryNode, ValueType.class).getValueType());
            irUnaryMathNode.setUnaryType(unaryType);
            irUnaryMathNode.setOperation(userUnaryNode.getOperation());
            irUnaryMathNode.setOriginallyExplicit(scriptScope.getCondition(userUnaryNode, Explicit.class));
            irUnaryMathNode.setChildNode(injectCast(userUnaryNode.getChildNode(), scriptScope));
            irNode = irUnaryMathNode;
        }

        scriptScope.putDecoration(userUnaryNode, new IRNodeDecoration(irNode));
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        Operation operation = userBinaryNode.getOperation();
        Class<?> valueType = scriptScope.getDecoration(userBinaryNode, ValueType.class).getValueType();

        if (operation == Operation.ADD && valueType == String.class) {
            StringConcatenationNode stringConcatenationNode = new StringConcatenationNode();
            stringConcatenationNode.setCat(scriptScope.getCondition(userBinaryNode, Concatenate.class));
            stringConcatenationNode.addArgumentNode((ExpressionNode)visit(userBinaryNode.getLeftNode(), scriptScope));
            stringConcatenationNode.addArgumentNode((ExpressionNode)visit(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = stringConcatenationNode;
        } else {
            Class<?> shiftType = scriptScope.hasDecoration(userBinaryNode, ShiftType.class) ?
                    scriptScope.getDecoration(userBinaryNode, ShiftType.class).getShiftType() : null;

            BinaryMathNode irBinaryMathNode = new BinaryMathNode();

            irBinaryMathNode.setBinaryType(scriptScope.getDecoration(userBinaryNode, BinaryType.class).getBinaryType());
            irBinaryMathNode.setShiftType(shiftType);
            irBinaryMathNode.setOperation(operation);

            if (scriptScope.getCondition(userBinaryNode, Explicit.class)) {
                irBinaryMathNode.setFlags(DefBootstrap.OPERATOR_EXPLICIT_CAST);
            }

            irBinaryMathNode.setLeftNode(injectCast(userBinaryNode.getLeftNode(), scriptScope));
            irBinaryMathNode.setRightNode(injectCast(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = irBinaryMathNode;
        }

        irExpressionNode.setLocation(userBinaryNode.getLocation());
        irExpressionNode.setExpressionType(valueType);
        scriptScope.putDecoration(userBinaryNode, new IRNodeDecoration(irExpressionNode));
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, ScriptScope scriptScope) {
        BooleanNode irBooleanNode = new BooleanNode();
        irBooleanNode.setLocation(userBooleanCompNode.getLocation());
        irBooleanNode.setExpressionType(scriptScope.getDecoration(userBooleanCompNode, ValueType.class).getValueType());
        irBooleanNode.setOperation(userBooleanCompNode.getOperation());
        irBooleanNode.setLeftNode(injectCast(userBooleanCompNode.getLeftNode(), scriptScope));
        irBooleanNode.setRightNode(injectCast(userBooleanCompNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userBooleanCompNode, new IRNodeDecoration(irBooleanNode));
    }

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComparisonNode = new ComparisonNode();
        irComparisonNode.setLocation(userCompNode.getLocation());
        irComparisonNode.setExpressionType(scriptScope.getDecoration(userCompNode, ValueType.class).getValueType());
        irComparisonNode.setComparisonType(scriptScope.getDecoration(userCompNode, ComparisonType.class).getComparisonType());
        irComparisonNode.setOperation(userCompNode.getOperation());
        irComparisonNode.setLeftNode(injectCast(userCompNode.getLeftNode(), scriptScope));
        irComparisonNode.setRightNode(injectCast(userCompNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userCompNode, new IRNodeDecoration(irComparisonNode));
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, ScriptScope scriptScope) {
        scriptScope.putDecoration(userExplicitNode, new IRNodeDecoration(injectCast(userExplicitNode.getChildNode(), scriptScope)));
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, ScriptScope scriptScope) {
        InstanceofNode irInstanceofNode = new InstanceofNode();
        irInstanceofNode.setLocation(userInstanceofNode.getLocation());
        irInstanceofNode.setExpressionType(scriptScope.getDecoration(userInstanceofNode, ValueType.class).getValueType());
        irInstanceofNode.setInstanceType(scriptScope.getDecoration(userInstanceofNode, InstanceType.class).getInstanceType());
        irInstanceofNode.setChildNode((ExpressionNode)visit(userInstanceofNode.getExpressionNode(), scriptScope));

        scriptScope.putDecoration(userInstanceofNode, new IRNodeDecoration(irInstanceofNode));
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, ScriptScope scriptScope) {
        ConditionalNode irConditionalNode = new ConditionalNode();
        irConditionalNode.setLocation(userConditionalNode.getLocation());
        irConditionalNode.setExpressionType(scriptScope.getDecoration(userConditionalNode, ValueType.class).getValueType());
        irConditionalNode.setConditionNode(injectCast(userConditionalNode.getConditionNode(), scriptScope));
        irConditionalNode.setLeftNode(injectCast(userConditionalNode.getTrueNode(), scriptScope));
        irConditionalNode.setRightNode(injectCast(userConditionalNode.getFalseNode(), scriptScope));

        scriptScope.putDecoration(userConditionalNode, new IRNodeDecoration(irConditionalNode));
    }

    @Override
    public void visitElvis(EElvis userElvisNode, ScriptScope scriptScope) {
        ElvisNode irElvisNode = new ElvisNode();
        irElvisNode.setLocation(userElvisNode.getLocation());
        irElvisNode.setExpressionType(scriptScope.getDecoration(userElvisNode, ValueType.class).getValueType());
        irElvisNode.setLeftNode(injectCast(userElvisNode.getLeftNode(), scriptScope));
        irElvisNode.setRightNode(injectCast(userElvisNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userElvisNode, new IRNodeDecoration(irElvisNode));
    }

    @Override
    public void visitListInit(EListInit userListInitNode, ScriptScope scriptScope) {
        ListInitializationNode irListInitializationNode = new ListInitializationNode();

        irListInitializationNode.setLocation(userListInitNode.getLocation());
        irListInitializationNode.setExpressionType(scriptScope.getDecoration(userListInitNode, ValueType.class).getValueType());
        irListInitializationNode.setConstructor(
                scriptScope.getDecoration(userListInitNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());
        irListInitializationNode.setMethod(
                scriptScope.getDecoration(userListInitNode, StandardPainlessMethod.class).getStandardPainlessMethod());

        for (AExpression userValueNode : userListInitNode.getValueNodes()) {
            irListInitializationNode.addArgumentNode(injectCast(userValueNode, scriptScope));
        }

        scriptScope.putDecoration(userListInitNode, new IRNodeDecoration(irListInitializationNode));
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, ScriptScope scriptScope) {
        MapInitializationNode irMapInitializationNode = new MapInitializationNode();

        irMapInitializationNode.setLocation(userMapInitNode.getLocation());
        irMapInitializationNode.setExpressionType(scriptScope.getDecoration(userMapInitNode, ValueType.class).getValueType());
        irMapInitializationNode.setConstructor(
                scriptScope.getDecoration(userMapInitNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());
        irMapInitializationNode.setMethod(
                scriptScope.getDecoration(userMapInitNode, StandardPainlessMethod.class).getStandardPainlessMethod());


        for (int i = 0; i < userMapInitNode.getKeyNodes().size(); ++i) {
            irMapInitializationNode.addArgumentNode(
                    injectCast(userMapInitNode.getKeyNodes().get(i), scriptScope),
                    injectCast(userMapInitNode.getValueNodes().get(i), scriptScope));
        }

        scriptScope.putDecoration(userMapInitNode, new IRNodeDecoration(irMapInitializationNode));
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, ScriptScope scriptScope) {
        NewArrayNode irNewArrayNode = new NewArrayNode();

        irNewArrayNode.setLocation(userNewArrayNode.getLocation());
        irNewArrayNode.setExpressionType(scriptScope.getDecoration(userNewArrayNode, ValueType.class).getValueType());
        irNewArrayNode.setInitialize(userNewArrayNode.isInitializer());

        for (AExpression userArgumentNode : userNewArrayNode.getValueNodes()) {
            irNewArrayNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewArrayNode, new IRNodeDecoration(irNewArrayNode));
    }

    @Override
    public void visitNewObj(ENewObj userNewObjectNode, ScriptScope scriptScope) {
        NewObjectNode irNewObjectNode = new NewObjectNode();

        irNewObjectNode.setLocation(userNewObjectNode.getLocation());
        irNewObjectNode.setExpressionType(scriptScope.getDecoration(userNewObjectNode, ValueType.class).getValueType());
        irNewObjectNode.setRead(scriptScope.getCondition(userNewObjectNode, Read.class));
        irNewObjectNode.setConstructor(
                scriptScope.getDecoration(userNewObjectNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());

        for (AExpression userArgumentNode : userNewObjectNode.getArgumentNodes()) {
            irNewObjectNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewObjectNode, new IRNodeDecoration(irNewObjectNode));
    }

    @Override
    public void visitCallLocal(ECallLocal callLocalNode, ScriptScope scriptScope) {
        InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode();

        if (scriptScope.hasDecoration(callLocalNode, StandardLocalFunction.class)) {
            irInvokeCallMemberNode.setLocalFunction(
                    scriptScope.getDecoration(callLocalNode, StandardLocalFunction.class).getLocalFunction());
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessMethod.class)) {
            irInvokeCallMemberNode.setImportedMethod(
                    scriptScope.getDecoration(callLocalNode, StandardPainlessMethod.class).getStandardPainlessMethod());
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessClassBinding.class)) {
            PainlessClassBinding painlessClassBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessClassBinding.class).getPainlessClassBinding();
            String bindingName = scriptScope.getNextSyntheticName("class_binding");

            FieldNode irFieldNode = new FieldNode();
            irFieldNode.setLocation(callLocalNode.getLocation());
            irFieldNode.setModifiers(Modifier.PRIVATE);
            irFieldNode.setFieldType(painlessClassBinding.javaConstructor.getDeclaringClass());
            irFieldNode.setName(bindingName);
            irClassNode.addFieldNode(irFieldNode);

            irInvokeCallMemberNode.setClassBinding(painlessClassBinding);
            irInvokeCallMemberNode.setClassBindingOffset(
                    (int)scriptScope.getDecoration(callLocalNode, StandardConstant.class).getStandardConstant());
            irInvokeCallMemberNode.setBindingName(bindingName);
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessInstanceBinding.class)) {
            PainlessInstanceBinding painlessInstanceBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessInstanceBinding.class).getPainlessInstanceBinding();
            String bindingName = scriptScope.getNextSyntheticName("instance_binding");

            FieldNode irFieldNode = new FieldNode();
            irFieldNode.setLocation(callLocalNode.getLocation());
            irFieldNode.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
            irFieldNode.setFieldType(painlessInstanceBinding.targetInstance.getClass());
            irFieldNode.setName(bindingName);
            irClassNode.addFieldNode(irFieldNode);

            irInvokeCallMemberNode.setInstanceBinding(painlessInstanceBinding);
            irInvokeCallMemberNode.setBindingName(bindingName);

            scriptScope.addStaticConstant(bindingName, painlessInstanceBinding.targetInstance);
        } else {
            throw callLocalNode.createError(new IllegalStateException("illegal tree structure"));
        }

        for (AExpression userArgumentNode : callLocalNode.getArgumentNodes()) {
            irInvokeCallMemberNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        irInvokeCallMemberNode.setLocation(callLocalNode.getLocation());
        irInvokeCallMemberNode.setExpressionType(scriptScope.getDecoration(callLocalNode, ValueType.class).getValueType());

        scriptScope.putDecoration(callLocalNode, new IRNodeDecoration(irInvokeCallMemberNode));
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userBooleanConstantNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userBooleanConstantNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userBooleanConstantNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userBooleanConstantNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userNumericNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userNumericNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userNumericNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userNumericNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userDecimalNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userDecimalNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userDecimalNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userDecimalNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitString(EString userStringNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userStringNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userStringNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userStringNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userStringNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNull(ENull userNullNode, ScriptScope scriptScope) {
        NullNode irNullNode = new NullNode();
        irNullNode.setLocation(userNullNode.getLocation());
        irNullNode.setExpressionType(scriptScope.getDecoration(userNullNode, ValueType.class).getValueType());

        scriptScope.putDecoration(userNullNode, new IRNodeDecoration(irNullNode));
    }

    @Override
    public void visitRegex(ERegex userRegexNode, ScriptScope scriptScope) {
        String memberFieldName = scriptScope.getNextSyntheticName("regex");

        FieldNode irFieldNode = new FieldNode();
        irFieldNode.setLocation(userRegexNode.getLocation());
        irFieldNode.setModifiers(Modifier.FINAL | Modifier.STATIC | Modifier.PRIVATE);
        irFieldNode.setFieldType(Pattern.class);
        irFieldNode.setName(memberFieldName);

        irClassNode.addFieldNode(irFieldNode);

        try {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode();
            irStatementExpressionNode.setLocation(userRegexNode.getLocation());

            BlockNode blockNode = irClassNode.getClinitBlockNode();
            blockNode.addStatementNode(irStatementExpressionNode);

            StoreFieldMemberNode irStoreFieldMemberNode = new StoreFieldMemberNode();
            irStoreFieldMemberNode.setLocation(userRegexNode.getLocation());
            irStoreFieldMemberNode.setExpressionType(void.class);
            irStoreFieldMemberNode.setStoreType(Pattern.class);
            irStoreFieldMemberNode.setName(memberFieldName);
            irStoreFieldMemberNode.setStatic(true);

            irStatementExpressionNode.setExpressionNode(irStoreFieldMemberNode);

            BinaryNode irBinaryNode = new BinaryNode();
            irBinaryNode.setLocation(userRegexNode.getLocation());
            irBinaryNode.setExpressionType(Pattern.class);

            irStoreFieldMemberNode.setChildNode(irBinaryNode);

            StaticNode irStaticNode = new StaticNode();
            irStaticNode.setLocation(userRegexNode.getLocation());
            irStaticNode.setExpressionType(Pattern.class);

            irBinaryNode.setLeftNode(irStaticNode);

            InvokeCallNode invokeCallNode = new InvokeCallNode();
            invokeCallNode.setLocation(userRegexNode.getLocation());
            invokeCallNode.setExpressionType(Pattern.class);
            invokeCallNode.setBox(Pattern.class);
            invokeCallNode.setMethod(new PainlessMethod(
                            Pattern.class.getMethod("compile", String.class, int.class),
                            Pattern.class,
                            Pattern.class,
                            Arrays.asList(String.class, int.class),
                            null,
                            null,
                            null
                    )
            );

            irBinaryNode.setRightNode(invokeCallNode);

            ConstantNode irConstantNode = new ConstantNode();
            irConstantNode.setLocation(userRegexNode.getLocation());
            irConstantNode.setExpressionType(String.class);
            irConstantNode.setConstant(userRegexNode.getPattern());

            invokeCallNode.addArgumentNode(irConstantNode);

            irConstantNode = new ConstantNode();
            irConstantNode.setLocation(userRegexNode.getLocation());
            irConstantNode.setExpressionType(int.class);
            irConstantNode.setConstant(scriptScope.getDecoration(userRegexNode, StandardConstant.class).getStandardConstant());

            invokeCallNode.addArgumentNode(irConstantNode);
        } catch (Exception exception) {
            throw userRegexNode.createError(new IllegalStateException("illegal tree structure"));
        }

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode();
        irLoadFieldMemberNode.setLocation(userRegexNode.getLocation());
        irLoadFieldMemberNode.setExpressionType(Pattern.class);
        irLoadFieldMemberNode.setName(memberFieldName);
        irLoadFieldMemberNode.setStatic(true);

        scriptScope.putDecoration(userRegexNode, new IRNodeDecoration(irLoadFieldMemberNode));
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userLambdaNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(scriptScope.getDecoration(userLambdaNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userLambdaNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setBlockNode((BlockNode)visit(userLambdaNode.getBlockNode(), scriptScope));
        irFunctionNode.setLocation(userLambdaNode.getLocation());
        irFunctionNode.setName(scriptScope.getDecoration(userLambdaNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(scriptScope.getDecoration(userLambdaNode, ReturnType.class).getReturnType());
        irFunctionNode.getTypeParameters().addAll(scriptScope.getDecoration(userLambdaNode, TypeParameters.class).getTypeParameters());
        irFunctionNode.getParameterNames().addAll(scriptScope.getDecoration(userLambdaNode, ParameterNames.class).getParameterNames());
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());
        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.setLocation(userLambdaNode.getLocation());
        irReferenceNode.setExpressionType(scriptScope.getDecoration(userLambdaNode, ValueType.class).getValueType());

        List<Variable> captures = scriptScope.getDecoration(userLambdaNode, CapturesDecoration.class).getCaptures();

        for (Variable capture : captures) {
            irReferenceNode.addCapture(capture.getName());
        }

        scriptScope.putDecoration(userLambdaNode, new IRNodeDecoration(irReferenceNode));
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        TargetType targetType = scriptScope.getDecoration(userFunctionRefNode, TargetType.class);
        CapturesDecoration capturesDecoration = scriptScope.getDecoration(userFunctionRefNode, CapturesDecoration.class);

        if (targetType == null) {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userFunctionRefNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        } else if (capturesDecoration != null && capturesDecoration.getCaptures().get(0).getType() == def.class) {
            TypedCaptureReferenceNode typedCaptureReferenceNode = new TypedCaptureReferenceNode();
            typedCaptureReferenceNode.setMethodName(userFunctionRefNode.getMethodName());
            irReferenceNode = typedCaptureReferenceNode;
        } else {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(
                    scriptScope.getDecoration(userFunctionRefNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        }

        irReferenceNode.setLocation(userFunctionRefNode.getLocation());
        irReferenceNode.setExpressionType(scriptScope.getDecoration(userFunctionRefNode, ValueType.class).getValueType());

        if (capturesDecoration != null) {
            irReferenceNode.addCapture(capturesDecoration.getCaptures().get(0).getName());
        }

        scriptScope.putDecoration(userFunctionRefNode, new IRNodeDecoration(irReferenceNode));
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userNewArrayFunctionRefNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(
                    scriptScope.getDecoration(userNewArrayFunctionRefNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userNewArrayFunctionRefNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        Class<?> returnType = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReturnType.class).getReturnType();

        LoadVariableNode irLoadVariableNode = new LoadVariableNode();
        irLoadVariableNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irLoadVariableNode.setExpressionType(int.class);
        irLoadVariableNode.setName("size");

        NewArrayNode irNewArrayNode = new NewArrayNode();
        irNewArrayNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irNewArrayNode.setExpressionType(returnType);
        irNewArrayNode.setInitialize(false);

        irNewArrayNode.addArgumentNode(irLoadVariableNode);

        ReturnNode irReturnNode = new ReturnNode();
        irReturnNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irReturnNode.setExpressionNode(irNewArrayNode);

        BlockNode irBlockNode = new BlockNode();
        irBlockNode.setAllEscape(true);
        irBlockNode.setStatementCount(1);
        irBlockNode.addStatementNode(irReturnNode);

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setMaxLoopCounter(0);
        irFunctionNode.setName(scriptScope.getDecoration(userNewArrayFunctionRefNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.addTypeParameter(int.class);
        irFunctionNode.addParameterName("size");
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setBlockNode(irBlockNode);

        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irReferenceNode.setExpressionType(scriptScope.getDecoration(userNewArrayFunctionRefNode, ValueType.class).getValueType());

        scriptScope.putDecoration(userNewArrayFunctionRefNode, new IRNodeDecoration(irReferenceNode));
    }

    /**
     * This handles both load and store for symbol accesses as necessary. This uses buildLoadStore to
     * stub out the appropriate load and store ir nodes.
     */
    @Override
    public void visitSymbol(ESymbol userSymbolNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userSymbolNode, StaticType.class)) {
            Class<?> staticType = scriptScope.getDecoration(userSymbolNode, StaticType.class).getStaticType();
            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(userSymbolNode.getLocation());
            staticNode.setExpressionType(staticType);
            irExpressionNode = staticNode;
        } else if (scriptScope.hasDecoration(userSymbolNode, ValueType.class)) {
            boolean read = scriptScope.getCondition(userSymbolNode, Read.class);
            boolean write = scriptScope.getCondition(userSymbolNode, Write.class);
            boolean compound = scriptScope.getCondition(userSymbolNode, Compound.class);
            Location location = userSymbolNode.getLocation();
            String symbol = userSymbolNode.getSymbol();
            Class<?> valueType = scriptScope.getDecoration(userSymbolNode, ValueType.class).getValueType();

            StoreNode irStoreNode = null;
            ExpressionNode irLoadNode = null;

            if (write || compound) {
                StoreVariableNode irStoreVariableNode = new StoreVariableNode();
                irStoreVariableNode.setLocation(location);
                irStoreVariableNode.setExpressionType(read ? valueType : void.class);
                irStoreVariableNode.setStoreType(valueType);
                irStoreVariableNode.setName(symbol);
                irStoreNode = irStoreVariableNode;
            }

            if (write == false || compound) {
                LoadVariableNode irLoadVariableNode = new LoadVariableNode();
                irLoadVariableNode.setLocation(location);
                irLoadVariableNode.setExpressionType(valueType);
                irLoadVariableNode.setName(symbol);
                irLoadNode = irLoadVariableNode;
            }

            scriptScope.putDecoration(userSymbolNode, new AccessDepth(0));
            irExpressionNode = buildLoadStore(0, location, false, null, null, irLoadNode, irStoreNode);
        } else {
            throw userSymbolNode.createError(new IllegalStateException("illegal tree structure"));
        }

        scriptScope.putDecoration(userSymbolNode, new IRNodeDecoration(irExpressionNode));
    }

    /**
     * This handles both load and store for dot accesses as necessary. This uses buildLoadStore to
     * stub out the appropriate load and store ir nodes.
     */
    @Override
    public void visitDot(EDot userDotNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userDotNode, StaticType.class)) {
            Class<?> staticType = scriptScope.getDecoration(userDotNode, StaticType.class).getStaticType();
            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(userDotNode.getLocation());
            staticNode.setExpressionType(staticType);
            irExpressionNode = staticNode;
        } else {
            boolean read = scriptScope.getCondition(userDotNode, Read.class);
            boolean write = scriptScope.getCondition(userDotNode, Write.class);
            boolean compound = scriptScope.getCondition(userDotNode, Compound.class);
            Location location = userDotNode.getLocation();
            String index = userDotNode.getIndex();
            Class<?> valueType = scriptScope.getDecoration(userDotNode, ValueType.class).getValueType();
            ValueType prefixValueType = scriptScope.getDecoration(userDotNode.getPrefixNode(), ValueType.class);

            ExpressionNode irPrefixNode = (ExpressionNode)visit(userDotNode.getPrefixNode(), scriptScope);
            ExpressionNode irIndexNode = null;
            StoreNode irStoreNode = null;
            ExpressionNode irLoadNode = null;
            int accessDepth;

            if (prefixValueType != null && prefixValueType.getValueType().isArray()) {
                LoadDotArrayLengthNode irLoadDotArrayLengthNode = new LoadDotArrayLengthNode();
                irLoadDotArrayLengthNode.setLocation(location);
                irLoadDotArrayLengthNode.setExpressionType(int.class);
                irLoadNode = irLoadDotArrayLengthNode;

                accessDepth = 1;
            } else if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
                if (write || compound) {
                    StoreDotDefNode irStoreDotDefNode = new StoreDotDefNode();
                    irStoreDotDefNode.setLocation(location);
                    irStoreDotDefNode.setExpressionType(read ? valueType : void.class);
                    irStoreDotDefNode.setStoreType(valueType);
                    irStoreDotDefNode.setValue(index);
                    irStoreNode = irStoreDotDefNode;
                }

                if (write == false || compound) {
                    LoadDotDefNode irLoadDotDefNode = new LoadDotDefNode();
                    irLoadDotDefNode.setLocation(location);
                    irLoadDotDefNode.setExpressionType(valueType);
                    irLoadDotDefNode.setValue(index);
                    irLoadNode = irLoadDotDefNode;
                }

                accessDepth = 1;
            } else if (scriptScope.hasDecoration(userDotNode, StandardPainlessField.class)) {
                PainlessField painlessField =
                        scriptScope.getDecoration(userDotNode, StandardPainlessField.class).getStandardPainlessField();

                if (write || compound) {
                    StoreDotNode irStoreDotNode = new StoreDotNode();
                    irStoreDotNode.setLocation(location);
                    irStoreDotNode.setExpressionType(read ? valueType : void.class);
                    irStoreDotNode.setStoreType(valueType);
                    irStoreDotNode.setField(painlessField);
                    irStoreNode = irStoreDotNode;
                }

                if (write == false || compound) {
                    LoadDotNode irLoadDotNode = new LoadDotNode();
                    irLoadDotNode.setLocation(location);
                    irLoadDotNode.setExpressionType(valueType);
                    irLoadDotNode.setField(painlessField);
                    irLoadNode = irLoadDotNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, Shortcut.class)) {
                if (write || compound) {
                    StoreDotShortcutNode irStoreDotShortcutNode = new StoreDotShortcutNode();
                    irStoreDotShortcutNode.setLocation(location);
                    irStoreDotShortcutNode.setExpressionType(read ? valueType : void.class);
                    irStoreDotShortcutNode.setStoreType(valueType);
                    irStoreDotShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreDotShortcutNode;
                }

                if (write == false || compound) {
                    LoadDotShortcutNode irLoadDotShortcutNode = new LoadDotShortcutNode();
                    irLoadDotShortcutNode.setLocation(location);
                    irLoadDotShortcutNode.setExpressionType(valueType);
                    irLoadDotShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadDotShortcutNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, MapShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode();
                irConstantNode.setLocation(location);
                irConstantNode.setExpressionType(String.class);
                irConstantNode.setConstant(index);
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode();
                    irStoreMapShortcutNode.setLocation(location);
                    irStoreMapShortcutNode.setExpressionType(read ? valueType : void.class);
                    irStoreMapShortcutNode.setStoreType(valueType);
                    irStoreMapShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreMapShortcutNode;
                }

                if (write == false || compound) {
                    LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode();
                    irLoadMapShortcutNode.setLocation(location);
                    irLoadMapShortcutNode.setExpressionType(valueType);
                    irLoadMapShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadMapShortcutNode;
                }

                accessDepth = 2;
            } else if (scriptScope.getCondition(userDotNode, ListShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode();
                irConstantNode.setLocation(location);
                irConstantNode.setExpressionType(int.class);
                irConstantNode.setConstant(scriptScope.getDecoration(userDotNode, StandardConstant.class).getStandardConstant());
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode();
                    irStoreListShortcutNode.setLocation(location);
                    irStoreListShortcutNode.setExpressionType(read ? valueType : void.class);
                    irStoreListShortcutNode.setStoreType(valueType);
                    irStoreListShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreListShortcutNode;
                }

                if (write == false || compound) {
                    LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode();
                    irLoadListShortcutNode.setLocation(location);
                    irLoadListShortcutNode.setExpressionType(valueType);
                    irLoadListShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadListShortcutNode;
                }

                accessDepth = 2;
            } else {
                throw userDotNode.createError(new IllegalStateException("illegal tree structure"));
            }

            scriptScope.putDecoration(userDotNode, new AccessDepth(accessDepth));
            irExpressionNode = buildLoadStore(
                    accessDepth, location, userDotNode.isNullSafe(), irPrefixNode, irIndexNode, irLoadNode, irStoreNode);
        }

        scriptScope.putDecoration(userDotNode, new IRNodeDecoration(irExpressionNode));
    }

    /**
     * This handles both load and store for brace accesses as necessary. This uses buildLoadStore to
     * stub out the appropriate load and store ir nodes.
     */
    @Override
    public void visitBrace(EBrace userBraceNode, ScriptScope scriptScope) {
        boolean read = scriptScope.getCondition(userBraceNode, Read.class);
        boolean write = scriptScope.getCondition(userBraceNode, Write.class);
        boolean compound = scriptScope.getCondition(userBraceNode, Compound.class);
        Location location = userBraceNode.getLocation();
        Class<?> valueType = scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType();
        Class<?> prefixValueType = scriptScope.getDecoration(userBraceNode.getPrefixNode(), ValueType.class).getValueType();

        ExpressionNode irPrefixNode = (ExpressionNode)visit(userBraceNode.getPrefixNode(), scriptScope);
        ExpressionNode irIndexNode = injectCast(userBraceNode.getIndexNode(), scriptScope);
        StoreNode irStoreNode = null;
        ExpressionNode irLoadNode = null;

        if (prefixValueType.isArray()) {
            FlipArrayIndexNode irFlipArrayIndexNode = new FlipArrayIndexNode();
            irFlipArrayIndexNode.setLocation(userBraceNode.getIndexNode().getLocation());
            irFlipArrayIndexNode.setExpressionType(int.class);
            irFlipArrayIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipArrayIndexNode;

            if (write || compound) {
                StoreBraceNode irStoreBraceNode = new StoreBraceNode();
                irStoreBraceNode.setLocation(location);
                irStoreBraceNode.setExpressionType(read ? valueType : void.class);
                irStoreBraceNode.setStoreType(valueType);
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceNode irLoadBraceNode = new LoadBraceNode();
                irLoadBraceNode.setLocation(userBraceNode.getLocation());
                irLoadBraceNode.setExpressionType(valueType);
                irLoadNode = irLoadBraceNode;
            }
        } else if (prefixValueType == def.class) {
            Class<?> indexType = scriptScope.getDecoration(userBraceNode.getIndexNode(), ValueType.class).getValueType();
            FlipDefIndexNode irFlipDefIndexNode = new FlipDefIndexNode();
            irFlipDefIndexNode.setLocation(userBraceNode.getIndexNode().getLocation());
            irFlipDefIndexNode.setExpressionType(indexType);
            irFlipDefIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipDefIndexNode;

            if (write || compound) {
                StoreBraceDefNode irStoreBraceNode = new StoreBraceDefNode();
                irStoreBraceNode.setLocation(location);
                irStoreBraceNode.setExpressionType(read ? valueType : void.class);
                irStoreBraceNode.setStoreType(valueType);
                irStoreBraceNode.setIndexType(indexType);
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceDefNode irLoadBraceDefNode = new LoadBraceDefNode();
                irLoadBraceDefNode.setLocation(userBraceNode.getLocation());
                irLoadBraceDefNode.setExpressionType(valueType);
                irLoadBraceDefNode.setIndexType(indexType);
                irLoadNode = irLoadBraceDefNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, MapShortcut.class)) {
            if (write || compound) {
                StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode();
                irStoreMapShortcutNode.setLocation(location);
                irStoreMapShortcutNode.setExpressionType(read ? valueType : void.class);
                irStoreMapShortcutNode.setStoreType(valueType);
                irStoreMapShortcutNode.setSetter(
                        scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                irStoreNode = irStoreMapShortcutNode;
            }

            if (write == false || compound) {
                LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode();
                irLoadMapShortcutNode.setLocation(userBraceNode.getLocation());
                irLoadMapShortcutNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());
                irLoadMapShortcutNode.setGetter(
                        scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                irLoadNode = irLoadMapShortcutNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, ListShortcut.class)) {
            FlipCollectionIndexNode irFlipCollectionIndexNode = new FlipCollectionIndexNode();
            irFlipCollectionIndexNode.setLocation(userBraceNode.getIndexNode().getLocation());
            irFlipCollectionIndexNode.setExpressionType(int.class);
            irFlipCollectionIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipCollectionIndexNode;

            if (write || compound) {
                StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode();
                irStoreListShortcutNode.setLocation(location);
                irStoreListShortcutNode.setExpressionType(read ? valueType : void.class);
                irStoreListShortcutNode.setStoreType(valueType);
                irStoreListShortcutNode.setSetter(
                        scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                irStoreNode = irStoreListShortcutNode;
            }

            if (write == false || compound) {
                LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode();
                irLoadListShortcutNode.setLocation(userBraceNode.getLocation());
                irLoadListShortcutNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());
                irLoadListShortcutNode.setGetter(
                        scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                irLoadNode = irLoadListShortcutNode;
            }
        } else {
            throw userBraceNode.createError(new IllegalStateException("illegal tree structure"));
        }

        scriptScope.putDecoration(userBraceNode, new AccessDepth(2));

        scriptScope.putDecoration(userBraceNode, new IRNodeDecoration(
                buildLoadStore(2, location, false, irPrefixNode, irIndexNode, irLoadNode, irStoreNode)));
    }

    @Override
    public void visitCall(ECall userCallNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        ValueType prefixValueType = scriptScope.getDecoration(userCallNode.getPrefixNode(), ValueType.class);

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            InvokeCallDefNode irCallSubDefNode = new InvokeCallDefNode();

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                irCallSubDefNode.addArgumentNode((ExpressionNode)visit(userArgumentNode, scriptScope));
            }

            irCallSubDefNode.setLocation(userCallNode.getLocation());
            irCallSubDefNode.setExpressionType(scriptScope.getDecoration(userCallNode, ValueType.class).getValueType());
            irCallSubDefNode.setName(userCallNode.getMethodName());
            irExpressionNode = irCallSubDefNode;
        } else {
            Class<?> boxType;

            if (prefixValueType != null) {
                boxType = prefixValueType.getValueType();
            } else {
                boxType = scriptScope.getDecoration(userCallNode.getPrefixNode(), StaticType.class).getStaticType();
            }

            InvokeCallNode irInvokeCallNode = new InvokeCallNode();

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                irInvokeCallNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
            }

            irInvokeCallNode.setLocation(userCallNode.getLocation());
            irInvokeCallNode.setExpressionType(scriptScope.getDecoration(userCallNode, ValueType.class).getValueType());;
            irInvokeCallNode.setMethod(scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).getStandardPainlessMethod());
            irInvokeCallNode.setBox(boxType);
            irExpressionNode = irInvokeCallNode;
        }

        if (userCallNode.isNullSafe()) {
            NullSafeSubNode irNullSafeSubNode = new NullSafeSubNode();
            irNullSafeSubNode.setChildNode(irExpressionNode);
            irNullSafeSubNode.setLocation(irExpressionNode.getLocation());
            irNullSafeSubNode.setExpressionType(irExpressionNode.getExpressionType());
            irExpressionNode = irNullSafeSubNode;
        }

        BinaryNode irBinaryNode = new BinaryNode();
        irBinaryNode.setLeftNode((ExpressionNode)visit(userCallNode.getPrefixNode(), scriptScope));
        irBinaryNode.setRightNode(irExpressionNode);
        irBinaryNode.setLocation(irExpressionNode.getLocation());
        irBinaryNode.setExpressionType(irExpressionNode.getExpressionType());

        scriptScope.putDecoration(userCallNode, new IRNodeDecoration(irBinaryNode));
    }
}
