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
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
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
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
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
import org.elasticsearch.painless.symbol.IRDecorations.IRDBinaryType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFlags;
import org.elasticsearch.painless.symbol.IRDecorations.IRDOperation;
import org.elasticsearch.painless.symbol.IRDecorations.IRDRegexLimit;
import org.elasticsearch.painless.symbol.IRDecorations.IRDShiftType;
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
import java.util.Map;
import java.util.regex.Pattern;

public class DefaultUserTreeToIRTreePhase implements UserTreeVisitor<ScriptScope> {

    protected ClassNode irClassNode;

    /**
     * This injects additional ir nodes required for resolving the def type at runtime.
     * This includes injection of ir nodes to add a function to call
     * {@link DefBootstrap#bootstrap(PainlessLookup, FunctionTable, Map, Lookup, String, MethodType, int, int, Object...)}
     * to do the runtime resolution, and several supporting static fields.
     */
    protected void injectBootstrapMethod(ScriptScope scriptScope) {
        // adds static fields required for def bootstrapping
        Location internalLocation = new Location("$internal$injectStaticFields", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(PainlessLookup.class);
        irFieldNode.setName("$DEFINITION");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(FunctionTable.class);
        irFieldNode.setName("$FUNCTIONS");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(Map.class);
        irFieldNode.setName("$COMPILERSETTINGS");

        irClassNode.addFieldNode(irFieldNode);

        // adds the bootstrap method required for dynamic binding for def type resolution
        internalLocation = new Location("$internal$injectDefBootstrapMethod", 0);

        try {
            FunctionNode irFunctionNode = new FunctionNode(internalLocation);
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

            BlockNode blockNode = new BlockNode(internalLocation);
            blockNode.setAllEscape(true);

            irFunctionNode.setBlockNode(blockNode);

            ReturnNode returnNode = new ReturnNode(internalLocation);

            blockNode.addStatementNode(returnNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(internalLocation);
            irBinaryImplNode.attachDecoration(new IRDExpressionType(CallSite.class));

            returnNode.setExpressionNode(irBinaryImplNode);

            StaticNode staticNode = new StaticNode(internalLocation);
            staticNode.attachDecoration(new IRDExpressionType(DefBootstrap.class));

            irBinaryImplNode.setLeftNode(staticNode);

            InvokeCallNode invokeCallNode = new InvokeCallNode(internalLocation);
            invokeCallNode.attachDecoration(new IRDExpressionType(CallSite.class));
            invokeCallNode.setMethod(new PainlessMethod(
                            DefBootstrap.class.getMethod("bootstrap",
                                    PainlessLookup.class,
                                    FunctionTable.class,
                                    Map.class,
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
                                    Map.class,
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

            irBinaryImplNode.setRightNode(invokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(PainlessLookup.class));
            irLoadFieldMemberNode.setName("$DEFINITION");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(FunctionTable.class));
            irLoadFieldMemberNode.setName("$FUNCTIONS");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(Map.class));
            irLoadFieldMemberNode.setName("$COMPILERSETTINGS");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(Lookup.class));
            irLoadVariableNode.setName("methodHandlesLookup");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(String.class));
            irLoadVariableNode.setName("name");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(MethodType.class));
            irLoadVariableNode.setName("type");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(int.class));
            irLoadVariableNode.setName("initialDepth");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(int.class));
            irLoadVariableNode.setName("flavor");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(Object[].class));
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

        PainlessCast painlessCast = expressionPainlessCast.getExpressionPainlessCast();
        Class<?> targetType = painlessCast.targetType;

        if (painlessCast.boxTargetType != null) {
            targetType = PainlessLookupUtility.typeToBoxedType(painlessCast.boxTargetType);
        } else if (painlessCast.unboxTargetType != null) {
            targetType = painlessCast.unboxTargetType;
        }

        CastNode irCastNode = new CastNode(irExpressionNode.getLocation());
        irCastNode.attachDecoration(new IRDExpressionType(targetType));
        irCastNode.setCast(painlessCast);
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

        // build out the load structure for load/compound assignment or the store structure for just store
        ExpressionNode irExpressionNode = irLoadNode != null ? irLoadNode : irStoreNode;

        if (irPrefixNode != null) {
            // this load/store is a dot or brace load/store

            if (irIndexNode != null) {
                // this load/store requires an index
                BinaryImplNode binaryImplNode = new BinaryImplNode(location);

                if (isNullSafe) {
                    // the null-safe structure is slightly different from the standard structure since
                    // both the index and expression are not written to the stack if the prefix is null
                    binaryImplNode.attachDecoration(new IRDExpressionType(irExpressionNode.getDecorationValue(IRDExpressionType.class)));
                    binaryImplNode.setLeftNode(irIndexNode);
                    binaryImplNode.setRightNode(irExpressionNode);
                    irExpressionNode = binaryImplNode;
                } else {
                    binaryImplNode.attachDecoration(new IRDExpressionType(void.class));
                    binaryImplNode.setLeftNode(irPrefixNode);
                    binaryImplNode.setRightNode(irIndexNode);
                    irPrefixNode = binaryImplNode;
                }
            }

            if (irLoadNode != null && irStoreNode != null) {
                // this is a compound assignment and requires and additional dup to re-access the prefix
                DupNode dupNode = new DupNode(location);
                dupNode.attachDecoration(new IRDExpressionType(void.class));
                dupNode.setSize(accessDepth);
                dupNode.setDepth(0);
                dupNode.setChildNode(irPrefixNode);
                irPrefixNode = dupNode;
            }

            // build the structure to combine the prefix and the load/store
            BinaryImplNode binaryImplNode = new BinaryImplNode(location);
            binaryImplNode.attachDecoration(irExpressionNode.getDecoration(IRDExpressionType.class));

            if (isNullSafe) {
                // build the structure for a null safe load
                NullSafeSubNode irNullSafeSubNode = new NullSafeSubNode(location);
                irNullSafeSubNode.attachDecoration(irExpressionNode.getDecoration(IRDExpressionType.class));
                irNullSafeSubNode.setChildNode(irExpressionNode);
                binaryImplNode.setLeftNode(irPrefixNode);
                binaryImplNode.setRightNode(irNullSafeSubNode);
            } else {
                // build the structure for a standard load/store
                binaryImplNode.setLeftNode(irPrefixNode);
                binaryImplNode.setRightNode(irExpressionNode);
            }

            irExpressionNode = binaryImplNode;
        }

        if (irLoadNode != null && irStoreNode != null) {
            // this is a compound assignment and the store is the root
            irStoreNode.setChildNode(irExpressionNode);
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
        irClassNode = new ClassNode(userClassNode.getLocation());

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode)visit(userFunctionNode, scriptScope));
        }

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
                    ConstantNode irConstantNode = new ConstantNode(userFunctionNode.getLocation());
                    irConstantNode.attachDecoration(new IRDExpressionType(returnType));

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
                    irExpressionNode.attachDecoration(new IRDExpressionType(returnType));
                }
            } else {
                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
            }

            ReturnNode irReturnNode = new ReturnNode(userFunctionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);

            irBlockNode.addStatementNode(irReturnNode);
        }

        FunctionNode irFunctionNode = new FunctionNode(userFunctionNode.getLocation());
        irFunctionNode.setBlockNode(irBlockNode);
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
        BlockNode irBlockNode = new BlockNode(userBlockNode.getLocation());

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode)visit(userStatementNode, scriptScope));
        }

        irBlockNode.setAllEscape(scriptScope.getCondition(userBlockNode, AllEscape.class));

        scriptScope.putDecoration(userBlockNode, new IRNodeDecoration(irBlockNode));
    }

    @Override
    public void visitIf(SIf userIfNode, ScriptScope scriptScope) {
        IfNode irIfNode = new IfNode(userIfNode.getLocation());
        irIfNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfNode.setBlockNode((BlockNode)visit(userIfNode.getIfBlockNode(), scriptScope));

        scriptScope.putDecoration(userIfNode, new IRNodeDecoration(irIfNode));
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, ScriptScope scriptScope) {
        IfElseNode irIfElseNode = new IfElseNode(userIfElseNode.getLocation());
        irIfElseNode.setConditionNode(injectCast(userIfElseNode.getConditionNode(), scriptScope));
        irIfElseNode.setBlockNode((BlockNode)visit(userIfElseNode.getIfBlockNode(), scriptScope));
        irIfElseNode.setElseBlockNode((BlockNode)visit(userIfElseNode.getElseBlockNode(), scriptScope));

        scriptScope.putDecoration(userIfElseNode, new IRNodeDecoration(irIfElseNode));
    }

    @Override
    public void visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        WhileLoopNode irWhileLoopNode = new WhileLoopNode(userWhileNode.getLocation());
        irWhileLoopNode.setConditionNode(injectCast(userWhileNode.getConditionNode(), scriptScope));
        irWhileLoopNode.setBlockNode((BlockNode)visit(userWhileNode.getBlockNode(), scriptScope));
        irWhileLoopNode.setContinuous(scriptScope.getCondition(userWhileNode, ContinuousLoop.class));

        scriptScope.putDecoration(userWhileNode, new IRNodeDecoration(irWhileLoopNode));
    }

    @Override
    public void visitDo(SDo userDoNode, ScriptScope scriptScope) {
        DoWhileLoopNode irDoWhileLoopNode = new DoWhileLoopNode(userDoNode.getLocation());
        irDoWhileLoopNode.setConditionNode(injectCast(userDoNode.getConditionNode(), scriptScope));
        irDoWhileLoopNode.setBlockNode((BlockNode)visit(userDoNode.getBlockNode(), scriptScope));
        irDoWhileLoopNode.setContinuous(scriptScope.getCondition(userDoNode, ContinuousLoop.class));

        scriptScope.putDecoration(userDoNode, new IRNodeDecoration(irDoWhileLoopNode));
    }

    @Override
    public void visitFor(SFor userForNode, ScriptScope scriptScope) {
        ForLoopNode irForLoopNode = new ForLoopNode(userForNode.getLocation());
        irForLoopNode.setInitialzerNode(visit(userForNode.getInitializerNode(), scriptScope));
        irForLoopNode.setConditionNode(injectCast(userForNode.getConditionNode(), scriptScope));
        irForLoopNode.setAfterthoughtNode((ExpressionNode)visit(userForNode.getAfterthoughtNode(), scriptScope));
        irForLoopNode.setBlockNode((BlockNode)visit(userForNode.getBlockNode(), scriptScope));
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
            ForEachSubArrayNode irForEachSubArrayNode = new ForEachSubArrayNode(userEachNode.getLocation());
            irForEachSubArrayNode.setConditionNode(irIterableNode);
            irForEachSubArrayNode.setBlockNode(irBlockNode);
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
            ForEachSubIterableNode irForEachSubIterableNode = new ForEachSubIterableNode(userEachNode.getLocation());
            irForEachSubIterableNode.setConditionNode(irIterableNode);
            irForEachSubIterableNode.setBlockNode(irBlockNode);
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

        ForEachLoopNode irForEachLoopNode = new ForEachLoopNode(userEachNode.getLocation());
        irForEachLoopNode.setConditionNode(irConditionNode);

        scriptScope.putDecoration(userEachNode, new IRNodeDecoration(irForEachLoopNode));
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scriptScope) {
        DeclarationBlockNode irDeclarationBlockNode = new DeclarationBlockNode(userDeclBlockNode.getLocation());

        for (SDeclaration userDeclarationNode : userDeclBlockNode.getDeclarationNodes()) {
            irDeclarationBlockNode.addDeclarationNode((DeclarationNode)visit(userDeclarationNode, scriptScope));
        }

        scriptScope.putDecoration(userDeclBlockNode, new IRNodeDecoration(irDeclarationBlockNode));
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userDeclarationNode, SemanticVariable.class).getSemanticVariable();

        DeclarationNode irDeclarationNode = new DeclarationNode(userDeclarationNode.getLocation());
        irDeclarationNode.setExpressionNode(injectCast(userDeclarationNode.getValueNode(), scriptScope));
        irDeclarationNode.setDeclarationType(variable.getType());
        irDeclarationNode.setName(variable.getName());

        scriptScope.putDecoration(userDeclarationNode, new IRNodeDecoration(irDeclarationNode));
    }

    @Override
    public void visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        ReturnNode irReturnNode = new ReturnNode(userReturnNode.getLocation());
        irReturnNode.setExpressionNode(injectCast(userReturnNode.getValueNode(), scriptScope));

        scriptScope.putDecoration(userReturnNode, new IRNodeDecoration(irReturnNode));
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        StatementNode irStatementNode;
        ExpressionNode irExpressionNode = injectCast(userExpressionNode.getStatementNode(), scriptScope);

        if (scriptScope.getCondition(userExpressionNode, MethodEscape.class)) {
            ReturnNode irReturnNode = new ReturnNode(userExpressionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);
            irStatementNode = irReturnNode;
        } else {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode(userExpressionNode.getLocation());
            irStatementExpressionNode.setExpressionNode(irExpressionNode);
            irStatementNode = irStatementExpressionNode;
        }

        scriptScope.putDecoration(userExpressionNode, new IRNodeDecoration(irStatementNode));
    }

    @Override
    public void visitTry(STry userTryNode, ScriptScope scriptScope) {
        TryNode irTryNode = new TryNode(userTryNode.getLocation());

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            irTryNode.addCatchNode((CatchNode)visit(userCatchNode, scriptScope));
        }

        irTryNode.setBlockNode((BlockNode)visit(userTryNode.getBlockNode(), scriptScope));

        scriptScope.putDecoration(userTryNode, new IRNodeDecoration(irTryNode));
    }

    @Override
    public void visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userCatchNode, SemanticVariable.class).getSemanticVariable();

        CatchNode irCatchNode = new CatchNode(userCatchNode.getLocation());
        irCatchNode.setExceptionType(variable.getType());
        irCatchNode.setSymbol(variable.getName());
        irCatchNode.setBlockNode((BlockNode)visit(userCatchNode.getBlockNode(), scriptScope));

        scriptScope.putDecoration(userCatchNode, new IRNodeDecoration(irCatchNode));
    }

    @Override
    public void visitThrow(SThrow userThrowNode, ScriptScope scriptScope) {
        ThrowNode irThrowNode = new ThrowNode(userThrowNode.getLocation());
        irThrowNode.setExpressionNode(injectCast(userThrowNode.getExpressionNode(), scriptScope));

        scriptScope.putDecoration(userThrowNode, new IRNodeDecoration(irThrowNode));
    }

    @Override
    public void visitContinue(SContinue userContinueNode, ScriptScope scriptScope) {
        ContinueNode irContinueNode = new ContinueNode(userContinueNode.getLocation());

        scriptScope.putDecoration(userContinueNode, new IRNodeDecoration(irContinueNode));
    }

    @Override
    public void visitBreak(SBreak userBreakNode, ScriptScope scriptScope) {
        BreakNode irBreakNode = new BreakNode(userBreakNode.getLocation());

        scriptScope.putDecoration(userBreakNode, new IRNodeDecoration(irBreakNode));
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, ScriptScope scriptScope) {
        boolean read = scriptScope.getCondition(userAssignmentNode, Read.class);
        Class<?> compoundType = scriptScope.hasDecoration(userAssignmentNode, CompoundType.class) ?
                scriptScope.getDecoration(userAssignmentNode, CompoundType.class).getCompoundType() : null;

        ExpressionNode irAssignmentNode;
        // add a cast node if necessary for the value node for the assignment
        ExpressionNode irValueNode = injectCast(userAssignmentNode.getRightNode(), scriptScope);

        // handles a compound assignment using the stub generated from buildLoadStore
        if (compoundType != null) {
            boolean concatenate = userAssignmentNode.getOperation() == Operation.ADD && compoundType == String.class;
            scriptScope.setCondition(userAssignmentNode.getLeftNode(), Compound.class);
            StoreNode irStoreNode = (StoreNode)visit(userAssignmentNode.getLeftNode(), scriptScope);
            ExpressionNode irLoadNode = irStoreNode.getChildNode();
            ExpressionNode irCompoundNode;

            // handles when the operation is a string concatenation
            if (concatenate) {
                StringConcatenationNode stringConcatenationNode = new StringConcatenationNode(irStoreNode.getLocation());
                stringConcatenationNode.attachDecoration(new IRDExpressionType(String.class));
                irCompoundNode = stringConcatenationNode;

                // must handle the StringBuilder case for java version <= 8
                if (irLoadNode instanceof BinaryImplNode && WriterConstants.INDY_STRING_CONCAT_BOOTSTRAP_HANDLE == null) {
                    ((DupNode)((BinaryImplNode)irLoadNode).getLeftNode()).setDepth(1);
                }
            // handles when the operation is mathematical
            } else {
                BinaryMathNode irBinaryMathNode = new BinaryMathNode(irStoreNode.getLocation());
                irBinaryMathNode.setLeftNode(irLoadNode);
                irBinaryMathNode.attachDecoration(new IRDExpressionType(compoundType));
                irBinaryMathNode.attachDecoration(new IRDOperation(userAssignmentNode.getOperation()));
                irBinaryMathNode.attachDecoration(new IRDBinaryType(compoundType));
                // add a compound assignment flag to the binary math node
                irBinaryMathNode.attachDecoration(new IRDFlags(DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT));
                irCompoundNode = irBinaryMathNode;
            }

            PainlessCast downcast = scriptScope.hasDecoration(userAssignmentNode, DowncastPainlessCast.class) ?
                    scriptScope.getDecoration(userAssignmentNode, DowncastPainlessCast.class).getDowncastPainlessCast() : null;

            // no need to downcast so the binary math node is the value for the store node
            if (downcast == null) {
                irCompoundNode.attachDecoration(new IRDExpressionType(irStoreNode.getStoreType()));
                irStoreNode.setChildNode(irCompoundNode);
            // add a cast node to do a downcast as the value for the store node
            } else {
                CastNode irCastNode = new CastNode(irCompoundNode.getLocation());
                irCastNode.attachDecoration(new IRDExpressionType(downcast.targetType));
                irCastNode.setCast(downcast);
                irCastNode.setChildNode(irCompoundNode);
                irStoreNode.setChildNode(irCastNode);
            }

            // the value is also read from this assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).getAccessDepth();
                DupNode irDupNode;

                // the value is read from prior to assignment (post-increment)
                if (userAssignmentNode.postIfRead()) {
                    irDupNode = new DupNode(irLoadNode.getLocation());
                    irDupNode.attachDecoration(irLoadNode.getDecoration(IRDExpressionType.class));
                    irDupNode.setSize(MethodWriter.getType(irLoadNode.getDecorationValue(IRDExpressionType.class)).getSize());
                    irDupNode.setDepth(accessDepth);
                    irDupNode.setChildNode(irLoadNode);
                    irLoadNode = irDupNode;
                // the value is read from after the assignment (pre-increment/compound)
                } else {
                    irDupNode = new DupNode(irStoreNode.getLocation());
                    irDupNode.attachDecoration(new IRDExpressionType(irStoreNode.getStoreType()));
                    irDupNode.setSize(MethodWriter.getType(irStoreNode.getDecorationValue(IRDExpressionType.class)).getSize());
                    irDupNode.setDepth(accessDepth);
                    irDupNode.setChildNode(irStoreNode.getChildNode());
                    irStoreNode.setChildNode(irDupNode);
                }
            }

            PainlessCast upcast = scriptScope.hasDecoration(userAssignmentNode, UpcastPainlessCast.class) ?
                    scriptScope.getDecoration(userAssignmentNode, UpcastPainlessCast.class).getUpcastPainlessCast() : null;

            // upcast the stored value if necessary
            if (upcast != null) {
                CastNode irCastNode = new CastNode(irLoadNode.getLocation());
                irCastNode.attachDecoration(new IRDExpressionType(upcast.targetType));
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

            irAssignmentNode = irStoreNode;
        // handles a standard assignment
        } else {
            irAssignmentNode = (ExpressionNode)visit(userAssignmentNode.getLeftNode(), scriptScope);

            // the value is read from after the assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).getAccessDepth();

                DupNode irDupNode = new DupNode(irValueNode.getLocation());
                irDupNode.attachDecoration(irValueNode.getDecoration(IRDExpressionType.class));
                irDupNode.setSize(MethodWriter.getType(irValueNode.getDecorationValue(IRDExpressionType.class)).getSize());
                irDupNode.setDepth(accessDepth);
                irDupNode.setChildNode(irValueNode);
                irValueNode = irDupNode;
            }

            if (irAssignmentNode instanceof BinaryImplNode) {
                ((StoreNode)((BinaryImplNode)irAssignmentNode).getRightNode()).setChildNode(irValueNode);
            } else {
                ((StoreNode)irAssignmentNode).setChildNode(irValueNode);
            }
        }

        scriptScope.putDecoration(userAssignmentNode, new IRNodeDecoration(irAssignmentNode));
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, ScriptScope scriptScope) {
        Class<?> unaryType = scriptScope.hasDecoration(userUnaryNode, UnaryType.class) ?
                scriptScope.getDecoration(userUnaryNode, UnaryType.class).getUnaryType() : null;

        IRNode irNode;

        if (scriptScope.getCondition(userUnaryNode.getChildNode(), Negate.class)) {
            irNode = visit(userUnaryNode.getChildNode(), scriptScope);
        } else {
            UnaryMathNode irUnaryMathNode = new UnaryMathNode(userUnaryNode.getLocation());
            irUnaryMathNode.attachDecoration(
                    new IRDExpressionType(scriptScope.getDecoration(userUnaryNode, ValueType.class).getValueType()));
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
            StringConcatenationNode stringConcatenationNode = new StringConcatenationNode(userBinaryNode.getLocation());
            stringConcatenationNode.addArgumentNode((ExpressionNode)visit(userBinaryNode.getLeftNode(), scriptScope));
            stringConcatenationNode.addArgumentNode((ExpressionNode)visit(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = stringConcatenationNode;
        } else {
            Class<?> binaryType = scriptScope.getDecoration(userBinaryNode, BinaryType.class).getBinaryType();
            Class<?> shiftType = scriptScope.hasDecoration(userBinaryNode, ShiftType.class) ?
                    scriptScope.getDecoration(userBinaryNode, ShiftType.class).getShiftType() : null;

            BinaryMathNode irBinaryMathNode = new BinaryMathNode(userBinaryNode.getLocation());

            irBinaryMathNode.attachDecoration(new IRDOperation(operation));

            if (operation == Operation.MATCH || operation == Operation.FIND) {
                irBinaryMathNode.attachDecoration(new IRDRegexLimit(scriptScope.getCompilerSettings().getRegexLimitFactor()));
            }

            irBinaryMathNode.attachDecoration(new IRDBinaryType(binaryType));

            if (shiftType != null) {
                irBinaryMathNode.attachDecoration(new IRDShiftType(shiftType));
            }

            if (scriptScope.getCondition(userBinaryNode, Explicit.class)) {
                irBinaryMathNode.attachDecoration(new IRDFlags(DefBootstrap.OPERATOR_EXPLICIT_CAST));
            }

            irBinaryMathNode.setLeftNode(injectCast(userBinaryNode.getLeftNode(), scriptScope));
            irBinaryMathNode.setRightNode(injectCast(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = irBinaryMathNode;
        }

        irExpressionNode.attachDecoration(new IRDExpressionType(valueType));
        scriptScope.putDecoration(userBinaryNode, new IRNodeDecoration(irExpressionNode));
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userBooleanCompNode, ValueType.class).getValueType();

        BooleanNode irBooleanNode = new BooleanNode(userBooleanCompNode.getLocation());
        irBooleanNode.attachDecoration(new IRDExpressionType(valueType));
        irBooleanNode.setOperation(userBooleanCompNode.getOperation());
        irBooleanNode.setLeftNode(injectCast(userBooleanCompNode.getLeftNode(), scriptScope));
        irBooleanNode.setRightNode(injectCast(userBooleanCompNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userBooleanCompNode, new IRNodeDecoration(irBooleanNode));
    }

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComparisonNode = new ComparisonNode(userCompNode.getLocation());
        irComparisonNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userCompNode, ValueType.class).getValueType()));
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
        Class<?> valuetype = scriptScope.getDecoration(userInstanceofNode, ValueType.class).getValueType();
        Class<?> instanceType = scriptScope.getDecoration(userInstanceofNode, InstanceType.class).getInstanceType();

        InstanceofNode irInstanceofNode = new InstanceofNode(userInstanceofNode.getLocation());
        irInstanceofNode.attachDecoration(new IRDExpressionType(valuetype));
        irInstanceofNode.setInstanceType(instanceType);
        irInstanceofNode.setChildNode((ExpressionNode)visit(userInstanceofNode.getExpressionNode(), scriptScope));

        scriptScope.putDecoration(userInstanceofNode, new IRNodeDecoration(irInstanceofNode));
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, ScriptScope scriptScope) {
        ConditionalNode irConditionalNode = new ConditionalNode(userConditionalNode.getLocation());
        irConditionalNode.attachDecoration(
                new IRDExpressionType(scriptScope.getDecoration(userConditionalNode, ValueType.class).getValueType()));
        irConditionalNode.setConditionNode(injectCast(userConditionalNode.getConditionNode(), scriptScope));
        irConditionalNode.setLeftNode(injectCast(userConditionalNode.getTrueNode(), scriptScope));
        irConditionalNode.setRightNode(injectCast(userConditionalNode.getFalseNode(), scriptScope));

        scriptScope.putDecoration(userConditionalNode, new IRNodeDecoration(irConditionalNode));
    }

    @Override
    public void visitElvis(EElvis userElvisNode, ScriptScope scriptScope) {
        ElvisNode irElvisNode = new ElvisNode(userElvisNode.getLocation());
        irElvisNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userElvisNode, ValueType.class).getValueType()));
        irElvisNode.setLeftNode(injectCast(userElvisNode.getLeftNode(), scriptScope));
        irElvisNode.setRightNode(injectCast(userElvisNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userElvisNode, new IRNodeDecoration(irElvisNode));
    }

    @Override
    public void visitListInit(EListInit userListInitNode, ScriptScope scriptScope) {
        ListInitializationNode irListInitializationNode = new ListInitializationNode(userListInitNode.getLocation());

        irListInitializationNode.attachDecoration(
                new IRDExpressionType(scriptScope.getDecoration(userListInitNode, ValueType.class).getValueType()));
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
        MapInitializationNode irMapInitializationNode = new MapInitializationNode(userMapInitNode.getLocation());

        irMapInitializationNode.attachDecoration(
                new IRDExpressionType(scriptScope.getDecoration(userMapInitNode, ValueType.class).getValueType()));
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
        NewArrayNode irNewArrayNode = new NewArrayNode(userNewArrayNode.getLocation());

        irNewArrayNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userNewArrayNode, ValueType.class).getValueType()));
        irNewArrayNode.setInitialize(userNewArrayNode.isInitializer());

        for (AExpression userArgumentNode : userNewArrayNode.getValueNodes()) {
            irNewArrayNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewArrayNode, new IRNodeDecoration(irNewArrayNode));
    }

    @Override
    public void visitNewObj(ENewObj userNewObjectNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userNewObjectNode, ValueType.class).getValueType();
        PainlessConstructor painlessConstructor =
                scriptScope.getDecoration(userNewObjectNode, StandardPainlessConstructor.class).getStandardPainlessConstructor();

        NewObjectNode irNewObjectNode = new NewObjectNode(userNewObjectNode.getLocation());

        irNewObjectNode.attachDecoration(new IRDExpressionType(valueType));
        irNewObjectNode.setRead(scriptScope.getCondition(userNewObjectNode, Read.class));
        irNewObjectNode.setConstructor(painlessConstructor);

        for (AExpression userArgumentNode : userNewObjectNode.getArgumentNodes()) {
            irNewObjectNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewObjectNode, new IRNodeDecoration(irNewObjectNode));
    }

    @Override
    public void visitCallLocal(ECallLocal callLocalNode, ScriptScope scriptScope) {
        InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(callLocalNode.getLocation());

        if (scriptScope.hasDecoration(callLocalNode, StandardLocalFunction.class)) {
            LocalFunction localFunction = scriptScope.getDecoration(callLocalNode, StandardLocalFunction.class).getLocalFunction();
            irInvokeCallMemberNode.setLocalFunction(localFunction);
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessMethod.class)) {
            PainlessMethod importedMethod =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessMethod.class).getStandardPainlessMethod();
            irInvokeCallMemberNode.setImportedMethod(importedMethod);
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessClassBinding.class)) {
            PainlessClassBinding painlessClassBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessClassBinding.class).getPainlessClassBinding();
            String bindingName = scriptScope.getNextSyntheticName("class_binding");

            FieldNode irFieldNode = new FieldNode(callLocalNode.getLocation());
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

            FieldNode irFieldNode = new FieldNode(callLocalNode.getLocation());
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

        Class<?> valueType = scriptScope.getDecoration(callLocalNode, ValueType.class).getValueType();
        irInvokeCallMemberNode.attachDecoration(new IRDExpressionType(valueType));

        scriptScope.putDecoration(callLocalNode, new IRNodeDecoration(irInvokeCallMemberNode));
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userBooleanConstantNode, ValueType.class).getValueType();
        Object constant = scriptScope.getDecoration(userBooleanConstantNode, StandardConstant.class).getStandardConstant();

        ConstantNode irConstantNode = new ConstantNode(userBooleanConstantNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.setConstant(constant);

        scriptScope.putDecoration(userBooleanConstantNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userNumericNode, ValueType.class).getValueType();
        Object constant = scriptScope.getDecoration(userNumericNode, StandardConstant.class).getStandardConstant();

        ConstantNode irConstantNode = new ConstantNode(userNumericNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.setConstant(constant);

        scriptScope.putDecoration(userNumericNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userDecimalNode, ValueType.class).getValueType();
        Object constant = scriptScope.getDecoration(userDecimalNode, StandardConstant.class).getStandardConstant();

        ConstantNode irConstantNode = new ConstantNode(userDecimalNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.setConstant(constant);

        scriptScope.putDecoration(userDecimalNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitString(EString userStringNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userStringNode, ValueType.class).getValueType();
        Object constant = scriptScope.getDecoration(userStringNode, StandardConstant.class).getStandardConstant();

        ConstantNode irConstantNode = new ConstantNode(userStringNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.setConstant(constant);

        scriptScope.putDecoration(userStringNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNull(ENull userNullNode, ScriptScope scriptScope) {
        NullNode irNullNode = new NullNode(userNullNode.getLocation());
        irNullNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userNullNode, ValueType.class).getValueType()));

        scriptScope.putDecoration(userNullNode, new IRNodeDecoration(irNullNode));
    }

    @Override
    public void visitRegex(ERegex userRegexNode, ScriptScope scriptScope) {
        String memberFieldName = scriptScope.getNextSyntheticName("regex");

        FieldNode irFieldNode = new FieldNode(userRegexNode.getLocation());
        irFieldNode.setModifiers(Modifier.FINAL | Modifier.STATIC | Modifier.PRIVATE);
        irFieldNode.setFieldType(Pattern.class);
        irFieldNode.setName(memberFieldName);

        irClassNode.addFieldNode(irFieldNode);

        try {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode(userRegexNode.getLocation());

            BlockNode blockNode = irClassNode.getClinitBlockNode();
            blockNode.addStatementNode(irStatementExpressionNode);

            StoreFieldMemberNode irStoreFieldMemberNode = new StoreFieldMemberNode(userRegexNode.getLocation());
            irStoreFieldMemberNode.attachDecoration(new IRDExpressionType(void.class));
            irStoreFieldMemberNode.setStoreType(Pattern.class);
            irStoreFieldMemberNode.setName(memberFieldName);
            irStoreFieldMemberNode.setStatic(true);

            irStatementExpressionNode.setExpressionNode(irStoreFieldMemberNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(userRegexNode.getLocation());
            irBinaryImplNode.attachDecoration(new IRDExpressionType(Pattern.class));

            irStoreFieldMemberNode.setChildNode(irBinaryImplNode);

            StaticNode irStaticNode = new StaticNode(userRegexNode.getLocation());
            irStaticNode.attachDecoration(new IRDExpressionType(Pattern.class));

            irBinaryImplNode.setLeftNode(irStaticNode);

            InvokeCallNode invokeCallNode = new InvokeCallNode(userRegexNode.getLocation());
            invokeCallNode.attachDecoration(new IRDExpressionType(Pattern.class));
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

            irBinaryImplNode.setRightNode(invokeCallNode);

            ConstantNode irConstantNode = new ConstantNode(userRegexNode.getLocation());
            irConstantNode.attachDecoration(new IRDExpressionType(String.class));
            irConstantNode.setConstant(userRegexNode.getPattern());

            invokeCallNode.addArgumentNode(irConstantNode);

            irConstantNode = new ConstantNode(userRegexNode.getLocation());
            irConstantNode.attachDecoration(new IRDExpressionType(int.class));
            irConstantNode.setConstant(scriptScope.getDecoration(userRegexNode, StandardConstant.class).getStandardConstant());

            invokeCallNode.addArgumentNode(irConstantNode);
        } catch (Exception exception) {
            throw userRegexNode.createError(new IllegalStateException("illegal tree structure"));
        }

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(userRegexNode.getLocation());
        irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(Pattern.class));
        irLoadFieldMemberNode.setName(memberFieldName);
        irLoadFieldMemberNode.setStatic(true);

        scriptScope.putDecoration(userRegexNode, new IRNodeDecoration(irLoadFieldMemberNode));
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userLambdaNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(userLambdaNode.getLocation());
            typedInterfaceReferenceNode.setReference(scriptScope.getDecoration(userLambdaNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userLambdaNode.getLocation());
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userLambdaNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        FunctionNode irFunctionNode = new FunctionNode(userLambdaNode.getLocation());
        irFunctionNode.setBlockNode((BlockNode)visit(userLambdaNode.getBlockNode(), scriptScope));
        irFunctionNode.setName(scriptScope.getDecoration(userLambdaNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(scriptScope.getDecoration(userLambdaNode, ReturnType.class).getReturnType());
        irFunctionNode.getTypeParameters().addAll(scriptScope.getDecoration(userLambdaNode, TypeParameters.class).getTypeParameters());
        irFunctionNode.getParameterNames().addAll(scriptScope.getDecoration(userLambdaNode, ParameterNames.class).getParameterNames());
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());
        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userLambdaNode, ValueType.class).getValueType()));

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
            String encoding = scriptScope.getDecoration(userFunctionRefNode, EncodingDecoration.class).getEncoding();
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userFunctionRefNode.getLocation());
            defInterfaceReferenceNode.setDefReferenceEncoding(encoding);
            irReferenceNode = defInterfaceReferenceNode;
        } else if (capturesDecoration != null && capturesDecoration.getCaptures().get(0).getType() == def.class) {
            TypedCaptureReferenceNode typedCaptureReferenceNode = new TypedCaptureReferenceNode(userFunctionRefNode.getLocation());
            typedCaptureReferenceNode.setMethodName(userFunctionRefNode.getMethodName());
            irReferenceNode = typedCaptureReferenceNode;
        } else {
            FunctionRef reference = scriptScope.getDecoration(userFunctionRefNode, ReferenceDecoration.class).getReference();
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(userFunctionRefNode.getLocation());
            typedInterfaceReferenceNode.setReference(reference);
            irReferenceNode = typedInterfaceReferenceNode;
        }

        irReferenceNode.attachDecoration(
                new IRDExpressionType(scriptScope.getDecoration(userFunctionRefNode, ValueType.class).getValueType()));

        if (capturesDecoration != null) {
            irReferenceNode.addCapture(capturesDecoration.getCaptures().get(0).getName());
        }

        scriptScope.putDecoration(userFunctionRefNode, new IRNodeDecoration(irReferenceNode));
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userNewArrayFunctionRefNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode =
                    new TypedInterfaceReferenceNode(userNewArrayFunctionRefNode.getLocation());
            FunctionRef reference = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReferenceDecoration.class).getReference();
            typedInterfaceReferenceNode.setReference(reference);
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            String encoding = scriptScope.getDecoration(userNewArrayFunctionRefNode, EncodingDecoration.class).getEncoding();
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userNewArrayFunctionRefNode.getLocation());
            defInterfaceReferenceNode.setDefReferenceEncoding(encoding);
            irReferenceNode = defInterfaceReferenceNode;
        }

        Class<?> returnType = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReturnType.class).getReturnType();

        LoadVariableNode irLoadVariableNode = new LoadVariableNode(userNewArrayFunctionRefNode.getLocation());
        irLoadVariableNode.attachDecoration(new IRDExpressionType(int.class));
        irLoadVariableNode.setName("size");

        NewArrayNode irNewArrayNode = new NewArrayNode(userNewArrayFunctionRefNode.getLocation());
        irNewArrayNode.attachDecoration(new IRDExpressionType(returnType));
        irNewArrayNode.setInitialize(false);

        irNewArrayNode.addArgumentNode(irLoadVariableNode);

        ReturnNode irReturnNode = new ReturnNode(userNewArrayFunctionRefNode.getLocation());
        irReturnNode.setExpressionNode(irNewArrayNode);

        BlockNode irBlockNode = new BlockNode(userNewArrayFunctionRefNode.getLocation());
        irBlockNode.setAllEscape(true);
        irBlockNode.addStatementNode(irReturnNode);

        FunctionNode irFunctionNode = new FunctionNode(userNewArrayFunctionRefNode.getLocation());
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

        irReferenceNode.attachDecoration(
                new IRDExpressionType(scriptScope.getDecoration(userNewArrayFunctionRefNode, ValueType.class).getValueType()));

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
            StaticNode staticNode = new StaticNode(userSymbolNode.getLocation());
            staticNode.attachDecoration(new IRDExpressionType(staticType));
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
                StoreVariableNode irStoreVariableNode = new StoreVariableNode(location);
                irStoreVariableNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreVariableNode.setStoreType(valueType);
                irStoreVariableNode.setName(symbol);
                irStoreNode = irStoreVariableNode;
            }

            if (write == false || compound) {
                LoadVariableNode irLoadVariableNode = new LoadVariableNode(location);
                irLoadVariableNode.attachDecoration(new IRDExpressionType(valueType));
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
            StaticNode staticNode = new StaticNode(userDotNode.getLocation());
            staticNode.attachDecoration(new IRDExpressionType(staticType));
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
                LoadDotArrayLengthNode irLoadDotArrayLengthNode = new LoadDotArrayLengthNode(location);
                irLoadDotArrayLengthNode.attachDecoration(new IRDExpressionType(int.class));
                irLoadNode = irLoadDotArrayLengthNode;

                accessDepth = 1;
            } else if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
                if (write || compound) {
                    StoreDotDefNode irStoreDotDefNode = new StoreDotDefNode(location);
                    irStoreDotDefNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreDotDefNode.setStoreType(valueType);
                    irStoreDotDefNode.setValue(index);
                    irStoreNode = irStoreDotDefNode;
                }

                if (write == false || compound) {
                    LoadDotDefNode irLoadDotDefNode = new LoadDotDefNode(location);
                    irLoadDotDefNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadDotDefNode.setValue(index);
                    irLoadNode = irLoadDotDefNode;
                }

                accessDepth = 1;
            } else if (scriptScope.hasDecoration(userDotNode, StandardPainlessField.class)) {
                PainlessField painlessField =
                        scriptScope.getDecoration(userDotNode, StandardPainlessField.class).getStandardPainlessField();

                if (write || compound) {
                    StoreDotNode irStoreDotNode = new StoreDotNode(location);
                    irStoreDotNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreDotNode.setStoreType(valueType);
                    irStoreDotNode.setField(painlessField);
                    irStoreNode = irStoreDotNode;
                }

                if (write == false || compound) {
                    LoadDotNode irLoadDotNode = new LoadDotNode(location);
                    irLoadDotNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadDotNode.setField(painlessField);
                    irLoadNode = irLoadDotNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, Shortcut.class)) {
                if (write || compound) {
                    StoreDotShortcutNode irStoreDotShortcutNode = new StoreDotShortcutNode(location);
                    irStoreDotShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreDotShortcutNode.setStoreType(valueType);
                    irStoreDotShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreDotShortcutNode;
                }

                if (write == false || compound) {
                    LoadDotShortcutNode irLoadDotShortcutNode = new LoadDotShortcutNode(location);
                    irLoadDotShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadDotShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadDotShortcutNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, MapShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode(location);
                irConstantNode.attachDecoration(new IRDExpressionType(String.class));
                irConstantNode.setConstant(index);
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode(location);
                    irStoreMapShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreMapShortcutNode.setStoreType(valueType);
                    irStoreMapShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreMapShortcutNode;
                }

                if (write == false || compound) {
                    LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode(location);
                    irLoadMapShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadMapShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadMapShortcutNode;
                }

                accessDepth = 2;
            } else if (scriptScope.getCondition(userDotNode, ListShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode(location);
                irConstantNode.attachDecoration(new IRDExpressionType(int.class));
                irConstantNode.setConstant(scriptScope.getDecoration(userDotNode, StandardConstant.class).getStandardConstant());
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode(location);
                    irStoreListShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreListShortcutNode.setStoreType(valueType);
                    irStoreListShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreListShortcutNode;
                }

                if (write == false || compound) {
                    LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode(location);
                    irLoadListShortcutNode.attachDecoration(new IRDExpressionType(valueType));
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
            FlipArrayIndexNode irFlipArrayIndexNode = new FlipArrayIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipArrayIndexNode.attachDecoration(new IRDExpressionType(int.class));
            irFlipArrayIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipArrayIndexNode;

            if (write || compound) {
                StoreBraceNode irStoreBraceNode = new StoreBraceNode(location);
                irStoreBraceNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreBraceNode.setStoreType(valueType);
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceNode irLoadBraceNode = new LoadBraceNode(location);
                irLoadBraceNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadNode = irLoadBraceNode;
            }
        } else if (prefixValueType == def.class) {
            Class<?> indexType = scriptScope.getDecoration(userBraceNode.getIndexNode(), ValueType.class).getValueType();
            FlipDefIndexNode irFlipDefIndexNode = new FlipDefIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipDefIndexNode.attachDecoration(new IRDExpressionType(indexType));
            irFlipDefIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipDefIndexNode;

            if (write || compound) {
                StoreBraceDefNode irStoreBraceNode = new StoreBraceDefNode(location);
                irStoreBraceNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreBraceNode.setStoreType(valueType);
                irStoreBraceNode.setIndexType(indexType);
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceDefNode irLoadBraceDefNode = new LoadBraceDefNode(location);
                irLoadBraceDefNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadBraceDefNode.setIndexType(indexType);
                irLoadNode = irLoadBraceDefNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, MapShortcut.class)) {
            if (write || compound) {
                PainlessMethod setter = scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod();
                StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode(location);
                irStoreMapShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreMapShortcutNode.setStoreType(valueType);
                irStoreMapShortcutNode.setSetter(setter);
                irStoreNode = irStoreMapShortcutNode;
            }

            if (write == false || compound) {
                PainlessMethod getter = scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod();
                LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode(location);
                irLoadMapShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadMapShortcutNode.setGetter(getter);
                irLoadNode = irLoadMapShortcutNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, ListShortcut.class)) {
            FlipCollectionIndexNode irFlipCollectionIndexNode = new FlipCollectionIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipCollectionIndexNode.attachDecoration(new IRDExpressionType(int.class));
            irFlipCollectionIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipCollectionIndexNode;

            if (write || compound) {
                PainlessMethod setter = scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod();
                StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode(location);
                irStoreListShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreListShortcutNode.setStoreType(valueType);
                irStoreListShortcutNode.setSetter(setter);
                irStoreNode = irStoreListShortcutNode;
            }

            if (write == false || compound) {
                PainlessMethod getter = scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod();
                LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode(location);
                irLoadListShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadListShortcutNode.setGetter(getter);
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
        Class<?> valueType = scriptScope.getDecoration(userCallNode, ValueType.class).getValueType();

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            InvokeCallDefNode irCallSubDefNode = new InvokeCallDefNode(userCallNode.getLocation());

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                irCallSubDefNode.addArgumentNode((ExpressionNode)visit(userArgumentNode, scriptScope));
            }

            irCallSubDefNode.attachDecoration(new IRDExpressionType(valueType));
            irCallSubDefNode.setName(userCallNode.getMethodName());
            irExpressionNode = irCallSubDefNode;
        } else {
            Class<?> boxType;

            if (prefixValueType != null) {
                boxType = prefixValueType.getValueType();
            } else {
                boxType = scriptScope.getDecoration(userCallNode.getPrefixNode(), StaticType.class).getStaticType();
            }

            InvokeCallNode irInvokeCallNode = new InvokeCallNode(userCallNode.getLocation());
            PainlessMethod method = scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).getStandardPainlessMethod();
            Object[] injections = PainlessLookupUtility.buildInjections(method, scriptScope.getCompilerSettings().asMap());
            Class<?>[] parameterTypes = method.javaMethod.getParameterTypes();
            int augmentedOffset = method.javaMethod.getDeclaringClass() == method.targetClass ? 0 : 1;

            for (int i = 0; i < injections.length; i++) {
                Object injection = injections[i];
                Class<?> parameterType = parameterTypes[i + augmentedOffset];

                if (parameterType != PainlessLookupUtility.typeToUnboxedType(injection.getClass())) {
                    throw new IllegalStateException("illegal tree structure");
                }

                ConstantNode constantNode = new ConstantNode(userCallNode.getLocation());
                constantNode.attachDecoration(new IRDExpressionType(parameterType));
                constantNode.setConstant(injection);
                irInvokeCallNode.addArgumentNode(constantNode);
            }

            for (AExpression userCallArgumentNode : userCallNode.getArgumentNodes()) {
                irInvokeCallNode.addArgumentNode(injectCast(userCallArgumentNode, scriptScope));
            }

            irInvokeCallNode.attachDecoration(new IRDExpressionType(valueType));
            irInvokeCallNode.setMethod(scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).getStandardPainlessMethod());
            irInvokeCallNode.setBox(boxType);
            irExpressionNode = irInvokeCallNode;
        }

        if (userCallNode.isNullSafe()) {
            NullSafeSubNode irNullSafeSubNode = new NullSafeSubNode(irExpressionNode.getLocation());
            irNullSafeSubNode.setChildNode(irExpressionNode);
            irNullSafeSubNode.attachDecoration(irExpressionNode.getDecoration(IRDExpressionType.class));
            irExpressionNode = irNullSafeSubNode;
        }

        BinaryImplNode irBinaryImplNode = new BinaryImplNode(irExpressionNode.getLocation());
        irBinaryImplNode.setLeftNode((ExpressionNode)visit(userCallNode.getPrefixNode(), scriptScope));
        irBinaryImplNode.setRightNode(irExpressionNode);
        irBinaryImplNode.attachDecoration(irExpressionNode.getDecoration(IRDExpressionType.class));

        scriptScope.putDecoration(userCallNode, new IRNodeDecoration(irBinaryImplNode));
    }
}
