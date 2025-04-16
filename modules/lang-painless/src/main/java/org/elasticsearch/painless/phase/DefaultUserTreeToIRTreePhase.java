/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Def;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
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
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.StoreBraceDefNode;
import org.elasticsearch.painless.ir.StoreBraceNode;
import org.elasticsearch.painless.ir.StoreDotDefNode;
import org.elasticsearch.painless.ir.StoreDotNode;
import org.elasticsearch.painless.ir.StoreDotShortcutNode;
import org.elasticsearch.painless.ir.StoreListShortcutNode;
import org.elasticsearch.painless.ir.StoreMapShortcutNode;
import org.elasticsearch.painless.ir.StoreVariableNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.TypedCaptureReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.UnaryNode;
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
import org.elasticsearch.painless.symbol.Decorations.CaptureBox;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.ComparisonType;
import org.elasticsearch.painless.symbol.Decorations.Compound;
import org.elasticsearch.painless.symbol.Decorations.CompoundType;
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.DowncastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.DynamicInvocation;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.GetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.IRNodeDecoration;
import org.elasticsearch.painless.symbol.Decorations.InstanceCapturingFunctionRef;
import org.elasticsearch.painless.symbol.Decorations.InstanceCapturingLambda;
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
import org.elasticsearch.painless.symbol.Decorations.ThisPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.TypeParameters;
import org.elasticsearch.painless.symbol.Decorations.UnaryType;
import org.elasticsearch.painless.symbol.Decorations.UpcastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.IRDecorations.IRCAllEscape;
import org.elasticsearch.painless.symbol.IRDecorations.IRCCaptureBox;
import org.elasticsearch.painless.symbol.IRDecorations.IRCContinuous;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInitialize;
import org.elasticsearch.painless.symbol.IRDecorations.IRCInstanceCapture;
import org.elasticsearch.painless.symbol.IRDecorations.IRCRead;
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
        irFieldNode.attachDecoration(new IRDModifiers(modifiers));
        irFieldNode.attachDecoration(new IRDFieldType(PainlessLookup.class));
        irFieldNode.attachDecoration(new IRDName("$DEFINITION"));

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.attachDecoration(new IRDModifiers(modifiers));
        irFieldNode.attachDecoration(new IRDFieldType(FunctionTable.class));
        irFieldNode.attachDecoration(new IRDName("$FUNCTIONS"));

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.attachDecoration(new IRDModifiers(modifiers));
        irFieldNode.attachDecoration(new IRDFieldType(Map.class));
        irFieldNode.attachDecoration(new IRDName("$COMPILERSETTINGS"));

        irClassNode.addFieldNode(irFieldNode);

        // adds the bootstrap method required for dynamic binding for def type resolution
        internalLocation = new Location("$internal$injectDefBootstrapMethod", 0);

        try {
            FunctionNode irFunctionNode = new FunctionNode(internalLocation);
            irFunctionNode.attachDecoration(new IRDName("$bootstrapDef"));
            irFunctionNode.attachDecoration(new IRDReturnType(CallSite.class));
            irFunctionNode.attachDecoration(
                new IRDTypeParameters(List.of(Lookup.class, String.class, MethodType.class, int.class, int.class, Object[].class))
            );
            irFunctionNode.attachDecoration(
                new IRDParameterNames(List.of("methodHandlesLookup", "name", "type", "initialDepth", "flavor", "args"))
            );
            irFunctionNode.attachCondition(IRCStatic.class);
            irFunctionNode.attachCondition(IRCVarArgs.class);
            irFunctionNode.attachCondition(IRCSynthetic.class);
            irFunctionNode.attachDecoration(new IRDMaxLoopCounter(0));

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode blockNode = new BlockNode(internalLocation);
            blockNode.attachCondition(IRCAllEscape.class);

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
            invokeCallNode.setMethod(
                new PainlessMethod(
                    DefBootstrap.class.getMethod(
                        "bootstrap",
                        PainlessLookup.class,
                        FunctionTable.class,
                        Map.class,
                        Lookup.class,
                        String.class,
                        MethodType.class,
                        int.class,
                        int.class,
                        Object[].class
                    ),
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
                        Object[].class
                    ),
                    null,
                    null,
                    Map.of()
                )
            );
            invokeCallNode.setBox(DefBootstrap.class);

            irBinaryImplNode.setRightNode(invokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(PainlessLookup.class));
            irLoadFieldMemberNode.attachDecoration(new IRDName("$DEFINITION"));
            irLoadFieldMemberNode.attachCondition(IRCStatic.class);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(FunctionTable.class));
            irLoadFieldMemberNode.attachDecoration(new IRDName("$FUNCTIONS"));
            irLoadFieldMemberNode.attachCondition(IRCStatic.class);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.attachDecoration(new IRDExpressionType(Map.class));
            irLoadFieldMemberNode.attachDecoration(new IRDName("$COMPILERSETTINGS"));
            irLoadFieldMemberNode.attachCondition(IRCStatic.class);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(Lookup.class));
            irLoadVariableNode.attachDecoration(new IRDName("methodHandlesLookup"));

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(String.class));
            irLoadVariableNode.attachDecoration(new IRDName("name"));

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(MethodType.class));
            irLoadVariableNode.attachDecoration(new IRDName("type"));

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(int.class));
            irLoadVariableNode.attachDecoration(new IRDName("initialDepth"));

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(int.class));
            irLoadVariableNode.attachDecoration(new IRDName("flavor"));

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.attachDecoration(new IRDExpressionType(Object[].class));
            irLoadVariableNode.attachDecoration(new IRDName("args"));

            invokeCallNode.addArgumentNode(irLoadVariableNode);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    protected ExpressionNode injectCast(AExpression userExpressionNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode = (ExpressionNode) visit(userExpressionNode, scriptScope);

        if (irExpressionNode == null) {
            return null;
        }

        ExpressionPainlessCast expressionPainlessCast = scriptScope.getDecoration(userExpressionNode, ExpressionPainlessCast.class);

        if (expressionPainlessCast == null) {
            return irExpressionNode;
        }

        PainlessCast painlessCast = expressionPainlessCast.expressionPainlessCast();
        Class<?> targetType = painlessCast.targetType;

        if (painlessCast.boxTargetType != null) {
            targetType = PainlessLookupUtility.typeToBoxedType(painlessCast.boxTargetType);
        } else if (painlessCast.unboxTargetType != null) {
            targetType = painlessCast.unboxTargetType;
        }

        CastNode irCastNode = new CastNode(irExpressionNode.getLocation());
        irCastNode.attachDecoration(new IRDExpressionType(targetType));
        irCastNode.attachDecoration(new IRDCast(painlessCast));
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
    protected static ExpressionNode buildLoadStore(
        int accessDepth,
        Location location,
        boolean isNullSafe,
        ExpressionNode irPrefixNode,
        ExpressionNode irIndexNode,
        ExpressionNode irLoadNode,
        UnaryNode irStoreNode
    ) {

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
                dupNode.attachDecoration(new IRDSize(accessDepth));
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
            return scriptScope.getDecoration(userNode, IRNodeDecoration.class).irNode();
        }
    }

    @Override
    public void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        irClassNode = new ClassNode(userClassNode.getLocation());

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode) visit(userFunctionNode, scriptScope));
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

        BlockNode irBlockNode = (BlockNode) visit(userFunctionNode.getBlockNode(), scriptScope);

        if (methodEscape == false) {
            ExpressionNode irExpressionNode;

            if (returnType == void.class) {
                irExpressionNode = null;
            } else if (userFunctionNode.isAutoReturnEnabled()) {
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
            } else {
                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
            }

            ReturnNode irReturnNode = new ReturnNode(userFunctionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);

            irBlockNode.addStatementNode(irReturnNode);
        }

        FunctionNode irFunctionNode = new FunctionNode(userFunctionNode.getLocation());
        irFunctionNode.setBlockNode(irBlockNode);
        String mangledName = scriptScope.getFunctionTable()
            .getFunction(userFunctionNode.getFunctionName(), userFunctionNode.getCanonicalTypeNameParameters().size())
            .getMangledName();
        irFunctionNode.attachDecoration(new IRDName(mangledName));
        irFunctionNode.attachDecoration(new IRDReturnType(returnType));
        irFunctionNode.attachDecoration(new IRDTypeParameters(localFunction.getTypeParameters()));
        irFunctionNode.attachDecoration(new IRDParameterNames(userFunctionNode.getParameterNames()));

        if (userFunctionNode.isStatic()) {
            irFunctionNode.attachCondition(IRCStatic.class);
        }

        if (userFunctionNode.isSynthetic()) {
            irFunctionNode.attachCondition(IRCSynthetic.class);
        }

        irFunctionNode.attachDecoration(new IRDMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter()));

        scriptScope.putDecoration(userFunctionNode, new IRNodeDecoration(irFunctionNode));
    }

    @Override
    public void visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        BlockNode irBlockNode = new BlockNode(userBlockNode.getLocation());

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode) visit(userStatementNode, scriptScope));
        }

        if (scriptScope.getCondition(userBlockNode, AllEscape.class)) {
            irBlockNode.attachCondition(IRCAllEscape.class);
        }

        scriptScope.putDecoration(userBlockNode, new IRNodeDecoration(irBlockNode));
    }

    @Override
    public void visitIf(SIf userIfNode, ScriptScope scriptScope) {
        IfNode irIfNode = new IfNode(userIfNode.getLocation());
        irIfNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfNode.setBlockNode((BlockNode) visit(userIfNode.getIfBlockNode(), scriptScope));

        scriptScope.putDecoration(userIfNode, new IRNodeDecoration(irIfNode));
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, ScriptScope scriptScope) {
        IfElseNode irIfElseNode = new IfElseNode(userIfElseNode.getLocation());
        irIfElseNode.setConditionNode(injectCast(userIfElseNode.getConditionNode(), scriptScope));
        irIfElseNode.setBlockNode((BlockNode) visit(userIfElseNode.getIfBlockNode(), scriptScope));
        irIfElseNode.setElseBlockNode((BlockNode) visit(userIfElseNode.getElseBlockNode(), scriptScope));

        scriptScope.putDecoration(userIfElseNode, new IRNodeDecoration(irIfElseNode));
    }

    @Override
    public void visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        WhileLoopNode irWhileLoopNode = new WhileLoopNode(userWhileNode.getLocation());
        irWhileLoopNode.setConditionNode(injectCast(userWhileNode.getConditionNode(), scriptScope));
        irWhileLoopNode.setBlockNode((BlockNode) visit(userWhileNode.getBlockNode(), scriptScope));

        if (scriptScope.getCondition(userWhileNode, ContinuousLoop.class)) {
            irWhileLoopNode.attachCondition(IRCContinuous.class);
        }

        scriptScope.putDecoration(userWhileNode, new IRNodeDecoration(irWhileLoopNode));
    }

    @Override
    public void visitDo(SDo userDoNode, ScriptScope scriptScope) {
        DoWhileLoopNode irDoWhileLoopNode = new DoWhileLoopNode(userDoNode.getLocation());
        irDoWhileLoopNode.setConditionNode(injectCast(userDoNode.getConditionNode(), scriptScope));
        irDoWhileLoopNode.setBlockNode((BlockNode) visit(userDoNode.getBlockNode(), scriptScope));

        if (scriptScope.getCondition(userDoNode, ContinuousLoop.class)) {
            irDoWhileLoopNode.attachCondition(IRCContinuous.class);
        }

        scriptScope.putDecoration(userDoNode, new IRNodeDecoration(irDoWhileLoopNode));
    }

    @Override
    public void visitFor(SFor userForNode, ScriptScope scriptScope) {
        ForLoopNode irForLoopNode = new ForLoopNode(userForNode.getLocation());
        irForLoopNode.setInitializerNode(visit(userForNode.getInitializerNode(), scriptScope));
        irForLoopNode.setConditionNode(injectCast(userForNode.getConditionNode(), scriptScope));
        irForLoopNode.setAfterthoughtNode((ExpressionNode) visit(userForNode.getAfterthoughtNode(), scriptScope));
        irForLoopNode.setBlockNode((BlockNode) visit(userForNode.getBlockNode(), scriptScope));

        if (scriptScope.getCondition(userForNode, ContinuousLoop.class)) {
            irForLoopNode.attachCondition(IRCContinuous.class);
        }

        scriptScope.putDecoration(userForNode, new IRNodeDecoration(irForLoopNode));
    }

    @Override
    public void visitEach(SEach userEachNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userEachNode, SemanticVariable.class).semanticVariable();
        PainlessCast painlessCast = scriptScope.hasDecoration(userEachNode, ExpressionPainlessCast.class)
            ? scriptScope.getDecoration(userEachNode, ExpressionPainlessCast.class).expressionPainlessCast()
            : null;
        ExpressionNode irIterableNode = (ExpressionNode) visit(userEachNode.getIterableNode(), scriptScope);
        Class<?> iterableValueType = scriptScope.getDecoration(userEachNode.getIterableNode(), ValueType.class).valueType();
        BlockNode irBlockNode = (BlockNode) visit(userEachNode.getBlockNode(), scriptScope);

        ConditionNode irConditionNode;

        if (iterableValueType.isArray()) {
            ForEachSubArrayNode irForEachSubArrayNode = new ForEachSubArrayNode(userEachNode.getLocation());
            irForEachSubArrayNode.setConditionNode(irIterableNode);
            irForEachSubArrayNode.setBlockNode(irBlockNode);
            irForEachSubArrayNode.attachDecoration(new IRDVariableType(variable.type()));
            irForEachSubArrayNode.attachDecoration(new IRDVariableName(variable.name()));
            irForEachSubArrayNode.attachDecoration(new IRDArrayType(iterableValueType));
            irForEachSubArrayNode.attachDecoration(new IRDArrayName("#array" + userEachNode.getLocation().getOffset()));
            irForEachSubArrayNode.attachDecoration(new IRDIndexType(int.class));
            irForEachSubArrayNode.attachDecoration(new IRDIndexName("#index" + userEachNode.getLocation().getOffset()));
            irForEachSubArrayNode.attachDecoration(new IRDIndexedType(iterableValueType.getComponentType()));

            if (painlessCast != null) {
                irForEachSubArrayNode.attachDecoration(new IRDCast(painlessCast));
            }

            irConditionNode = irForEachSubArrayNode;
        } else if (iterableValueType == def.class || Iterable.class.isAssignableFrom(iterableValueType)) {
            ForEachSubIterableNode irForEachSubIterableNode = new ForEachSubIterableNode(userEachNode.getLocation());
            irForEachSubIterableNode.setConditionNode(irIterableNode);
            irForEachSubIterableNode.setBlockNode(irBlockNode);
            irForEachSubIterableNode.attachDecoration(new IRDVariableType(variable.type()));
            irForEachSubIterableNode.attachDecoration(new IRDVariableName(variable.name()));
            irForEachSubIterableNode.attachDecoration(new IRDIterableName("#itr" + userEachNode.getLocation().getOffset()));

            if (iterableValueType != def.class) {
                irForEachSubIterableNode.attachDecoration(new IRDIterableType(Iterator.class));
                irForEachSubIterableNode.attachDecoration(
                    new IRDMethod(scriptScope.getDecoration(userEachNode, IterablePainlessMethod.class).iterablePainlessMethod())
                );

                if (painlessCast != null) {
                    irForEachSubIterableNode.attachDecoration(new IRDCast(painlessCast));
                }
            } else {
                // use ValueIterator as we could be iterating over an array directly
                irForEachSubIterableNode.attachDecoration(new IRDIterableType(ValueIterator.class));

                if (painlessCast != null && variable.type().isPrimitive() == false) {
                    irForEachSubIterableNode.attachDecoration(new IRDCast(painlessCast));
                }
            }

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
            irDeclarationBlockNode.addDeclarationNode((DeclarationNode) visit(userDeclarationNode, scriptScope));
        }

        scriptScope.putDecoration(userDeclBlockNode, new IRNodeDecoration(irDeclarationBlockNode));
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userDeclarationNode, SemanticVariable.class).semanticVariable();

        DeclarationNode irDeclarationNode = new DeclarationNode(userDeclarationNode.getLocation());
        irDeclarationNode.setExpressionNode(injectCast(userDeclarationNode.getValueNode(), scriptScope));
        irDeclarationNode.attachDecoration(new IRDDeclarationType(variable.type()));
        irDeclarationNode.attachDecoration(new IRDName(variable.name()));

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
            irTryNode.addCatchNode((CatchNode) visit(userCatchNode, scriptScope));
        }

        irTryNode.setBlockNode((BlockNode) visit(userTryNode.getBlockNode(), scriptScope));

        scriptScope.putDecoration(userTryNode, new IRNodeDecoration(irTryNode));
    }

    @Override
    public void visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userCatchNode, SemanticVariable.class).semanticVariable();

        CatchNode irCatchNode = new CatchNode(userCatchNode.getLocation());
        irCatchNode.attachDecoration(new IRDExceptionType(variable.type()));
        irCatchNode.attachDecoration(new IRDSymbol(variable.name()));
        irCatchNode.setBlockNode((BlockNode) visit(userCatchNode.getBlockNode(), scriptScope));

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
        Class<?> compoundType = scriptScope.hasDecoration(userAssignmentNode, CompoundType.class)
            ? scriptScope.getDecoration(userAssignmentNode, CompoundType.class).compoundType()
            : null;

        ExpressionNode irAssignmentNode;
        // add a cast node if necessary for the value node for the assignment
        ExpressionNode irValueNode = injectCast(userAssignmentNode.getRightNode(), scriptScope);

        // handles a compound assignment using the stub generated from buildLoadStore
        if (compoundType != null) {
            boolean concatenate = userAssignmentNode.getOperation() == Operation.ADD && compoundType == String.class;
            scriptScope.setCondition(userAssignmentNode.getLeftNode(), Compound.class);
            UnaryNode irStoreNode = (UnaryNode) visit(userAssignmentNode.getLeftNode(), scriptScope);
            ExpressionNode irLoadNode = irStoreNode.getChildNode();
            ExpressionNode irCompoundNode;

            // handles when the operation is a string concatenation
            if (concatenate) {
                StringConcatenationNode stringConcatenationNode = new StringConcatenationNode(irStoreNode.getLocation());
                stringConcatenationNode.attachDecoration(new IRDExpressionType(String.class));
                irCompoundNode = stringConcatenationNode;

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

            PainlessCast downcast = scriptScope.hasDecoration(userAssignmentNode, DowncastPainlessCast.class)
                ? scriptScope.getDecoration(userAssignmentNode, DowncastPainlessCast.class).downcastPainlessCast()
                : null;

            // no need to downcast so the binary math node is the value for the store node
            if (downcast == null) {
                irCompoundNode.attachDecoration(new IRDExpressionType(irStoreNode.getDecorationValue(IRDStoreType.class)));
                irStoreNode.setChildNode(irCompoundNode);
                // add a cast node to do a downcast as the value for the store node
            } else {
                CastNode irCastNode = new CastNode(irCompoundNode.getLocation());
                irCastNode.attachDecoration(new IRDExpressionType(downcast.targetType));
                irCastNode.attachDecoration(new IRDCast(downcast));
                irCastNode.setChildNode(irCompoundNode);
                irStoreNode.setChildNode(irCastNode);
            }

            // the value is also read from this assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).accessDepth();
                DupNode irDupNode;

                // the value is read from prior to assignment (post-increment)
                if (userAssignmentNode.postIfRead()) {
                    int size = MethodWriter.getType(irLoadNode.getDecorationValue(IRDExpressionType.class)).getSize();
                    irDupNode = new DupNode(irLoadNode.getLocation());
                    irDupNode.attachDecoration(irLoadNode.getDecoration(IRDExpressionType.class));
                    irDupNode.attachDecoration(new IRDSize(size));
                    irDupNode.attachDecoration(new IRDDepth(accessDepth));
                    irDupNode.setChildNode(irLoadNode);
                    irLoadNode = irDupNode;
                    // the value is read from after the assignment (pre-increment/compound)
                } else {
                    int size = MethodWriter.getType(irStoreNode.getDecorationValue(IRDExpressionType.class)).getSize();
                    irDupNode = new DupNode(irStoreNode.getLocation());
                    irDupNode.attachDecoration(new IRDExpressionType(irStoreNode.getDecorationValue(IRDStoreType.class)));
                    irDupNode.attachDecoration(new IRDSize(size));
                    irDupNode.attachDecoration(new IRDDepth(accessDepth));
                    irDupNode.setChildNode(irStoreNode.getChildNode());
                    irStoreNode.setChildNode(irDupNode);
                }
            }

            PainlessCast upcast = scriptScope.hasDecoration(userAssignmentNode, UpcastPainlessCast.class)
                ? scriptScope.getDecoration(userAssignmentNode, UpcastPainlessCast.class).upcastPainlessCast()
                : null;

            // upcast the stored value if necessary
            if (upcast != null) {
                CastNode irCastNode = new CastNode(irLoadNode.getLocation());
                irCastNode.attachDecoration(new IRDExpressionType(upcast.targetType));
                irCastNode.attachDecoration(new IRDCast(upcast));
                irCastNode.setChildNode(irLoadNode);
                irLoadNode = irCastNode;
            }

            if (concatenate) {
                StringConcatenationNode irStringConcatenationNode = (StringConcatenationNode) irCompoundNode;
                irStringConcatenationNode.addArgumentNode(irLoadNode);
                irStringConcatenationNode.addArgumentNode(irValueNode);
            } else {
                BinaryMathNode irBinaryMathNode = (BinaryMathNode) irCompoundNode;
                irBinaryMathNode.setLeftNode(irLoadNode);
                irBinaryMathNode.setRightNode(irValueNode);
            }

            irAssignmentNode = irStoreNode;
            // handles a standard assignment
        } else {
            irAssignmentNode = (ExpressionNode) visit(userAssignmentNode.getLeftNode(), scriptScope);

            // the value is read from after the assignment
            if (read) {
                int size = MethodWriter.getType(irValueNode.getDecorationValue(IRDExpressionType.class)).getSize();
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).accessDepth();

                DupNode irDupNode = new DupNode(irValueNode.getLocation());
                irDupNode.attachDecoration(irValueNode.getDecoration(IRDExpressionType.class));
                irDupNode.attachDecoration(new IRDSize(size));
                irDupNode.attachDecoration(new IRDDepth(accessDepth));
                irDupNode.setChildNode(irValueNode);
                irValueNode = irDupNode;
            }

            if (irAssignmentNode instanceof BinaryImplNode bin) {
                ((UnaryNode) bin.getRightNode()).setChildNode(irValueNode);
            } else {
                ((UnaryNode) irAssignmentNode).setChildNode(irValueNode);
            }
        }

        scriptScope.putDecoration(userAssignmentNode, new IRNodeDecoration(irAssignmentNode));
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, ScriptScope scriptScope) {
        Class<?> unaryType = scriptScope.hasDecoration(userUnaryNode, UnaryType.class)
            ? scriptScope.getDecoration(userUnaryNode, UnaryType.class).unaryType()
            : null;

        IRNode irNode;

        if (scriptScope.getCondition(userUnaryNode.getChildNode(), Negate.class)) {
            irNode = visit(userUnaryNode.getChildNode(), scriptScope);
        } else {
            UnaryMathNode irUnaryMathNode = new UnaryMathNode(userUnaryNode.getLocation());
            irUnaryMathNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userUnaryNode, ValueType.class).valueType()));

            if (unaryType != null) {
                irUnaryMathNode.attachDecoration(new IRDUnaryType(unaryType));
            }

            irUnaryMathNode.attachDecoration(new IRDOperation(userUnaryNode.getOperation()));

            if (scriptScope.getCondition(userUnaryNode, Explicit.class)) {
                irUnaryMathNode.attachDecoration(new IRDFlags(DefBootstrap.OPERATOR_EXPLICIT_CAST));
            }

            irUnaryMathNode.setChildNode(injectCast(userUnaryNode.getChildNode(), scriptScope));
            irNode = irUnaryMathNode;
        }

        scriptScope.putDecoration(userUnaryNode, new IRNodeDecoration(irNode));
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        Operation operation = userBinaryNode.getOperation();
        Class<?> valueType = scriptScope.getDecoration(userBinaryNode, ValueType.class).valueType();

        if (operation == Operation.ADD && valueType == String.class) {
            StringConcatenationNode stringConcatenationNode = new StringConcatenationNode(userBinaryNode.getLocation());
            stringConcatenationNode.addArgumentNode((ExpressionNode) visit(userBinaryNode.getLeftNode(), scriptScope));
            stringConcatenationNode.addArgumentNode((ExpressionNode) visit(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = stringConcatenationNode;
        } else {
            Class<?> binaryType = scriptScope.getDecoration(userBinaryNode, BinaryType.class).binaryType();
            Class<?> shiftType = scriptScope.hasDecoration(userBinaryNode, ShiftType.class)
                ? scriptScope.getDecoration(userBinaryNode, ShiftType.class).shiftType()
                : null;

            BinaryMathNode irBinaryMathNode = new BinaryMathNode(userBinaryNode.getLocation());

            irBinaryMathNode.attachDecoration(new IRDOperation(operation));

            if (operation == Operation.MATCH || operation == Operation.FIND) {
                irBinaryMathNode.attachDecoration(new IRDRegexLimit(scriptScope.getCompilerSettings().getAppliedRegexLimitFactor()));
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
        Class<?> valueType = scriptScope.getDecoration(userBooleanCompNode, ValueType.class).valueType();

        BooleanNode irBooleanNode = new BooleanNode(userBooleanCompNode.getLocation());
        irBooleanNode.attachDecoration(new IRDExpressionType(valueType));
        irBooleanNode.attachDecoration(new IRDOperation(userBooleanCompNode.getOperation()));
        irBooleanNode.setLeftNode(injectCast(userBooleanCompNode.getLeftNode(), scriptScope));
        irBooleanNode.setRightNode(injectCast(userBooleanCompNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userBooleanCompNode, new IRNodeDecoration(irBooleanNode));
    }

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComparisonNode = new ComparisonNode(userCompNode.getLocation());
        irComparisonNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userCompNode, ValueType.class).valueType()));
        irComparisonNode.attachDecoration(
            new IRDComparisonType(scriptScope.getDecoration(userCompNode, ComparisonType.class).comparisonType())
        );
        irComparisonNode.attachDecoration(new IRDOperation(userCompNode.getOperation()));
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
        Class<?> valuetype = scriptScope.getDecoration(userInstanceofNode, ValueType.class).valueType();
        Class<?> instanceType = scriptScope.getDecoration(userInstanceofNode, InstanceType.class).instanceType();

        InstanceofNode irInstanceofNode = new InstanceofNode(userInstanceofNode.getLocation());
        irInstanceofNode.attachDecoration(new IRDExpressionType(valuetype));
        irInstanceofNode.attachDecoration(new IRDInstanceType(instanceType));
        irInstanceofNode.setChildNode((ExpressionNode) visit(userInstanceofNode.getExpressionNode(), scriptScope));

        scriptScope.putDecoration(userInstanceofNode, new IRNodeDecoration(irInstanceofNode));
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, ScriptScope scriptScope) {
        ConditionalNode irConditionalNode = new ConditionalNode(userConditionalNode.getLocation());
        irConditionalNode.attachDecoration(
            new IRDExpressionType(scriptScope.getDecoration(userConditionalNode, ValueType.class).valueType())
        );
        irConditionalNode.setConditionNode(injectCast(userConditionalNode.getConditionNode(), scriptScope));
        irConditionalNode.setLeftNode(injectCast(userConditionalNode.getTrueNode(), scriptScope));
        irConditionalNode.setRightNode(injectCast(userConditionalNode.getFalseNode(), scriptScope));

        scriptScope.putDecoration(userConditionalNode, new IRNodeDecoration(irConditionalNode));
    }

    @Override
    public void visitElvis(EElvis userElvisNode, ScriptScope scriptScope) {
        ElvisNode irElvisNode = new ElvisNode(userElvisNode.getLocation());
        irElvisNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userElvisNode, ValueType.class).valueType()));
        irElvisNode.setLeftNode(injectCast(userElvisNode.getLeftNode(), scriptScope));
        irElvisNode.setRightNode(injectCast(userElvisNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userElvisNode, new IRNodeDecoration(irElvisNode));
    }

    @Override
    public void visitListInit(EListInit userListInitNode, ScriptScope scriptScope) {
        ListInitializationNode irListInitializationNode = new ListInitializationNode(userListInitNode.getLocation());

        irListInitializationNode.attachDecoration(
            new IRDExpressionType(scriptScope.getDecoration(userListInitNode, ValueType.class).valueType())
        );
        irListInitializationNode.attachDecoration(
            new IRDConstructor(scriptScope.getDecoration(userListInitNode, StandardPainlessConstructor.class).standardPainlessConstructor())
        );
        irListInitializationNode.attachDecoration(
            new IRDMethod(scriptScope.getDecoration(userListInitNode, StandardPainlessMethod.class).standardPainlessMethod())
        );

        for (AExpression userValueNode : userListInitNode.getValueNodes()) {
            irListInitializationNode.addArgumentNode(injectCast(userValueNode, scriptScope));
        }

        scriptScope.putDecoration(userListInitNode, new IRNodeDecoration(irListInitializationNode));
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, ScriptScope scriptScope) {
        MapInitializationNode irMapInitializationNode = new MapInitializationNode(userMapInitNode.getLocation());

        irMapInitializationNode.attachDecoration(
            new IRDExpressionType(scriptScope.getDecoration(userMapInitNode, ValueType.class).valueType())
        );
        irMapInitializationNode.attachDecoration(
            new IRDConstructor(scriptScope.getDecoration(userMapInitNode, StandardPainlessConstructor.class).standardPainlessConstructor())
        );
        irMapInitializationNode.attachDecoration(
            new IRDMethod(scriptScope.getDecoration(userMapInitNode, StandardPainlessMethod.class).standardPainlessMethod())
        );

        for (int i = 0; i < userMapInitNode.getKeyNodes().size(); ++i) {
            irMapInitializationNode.addArgumentNode(
                injectCast(userMapInitNode.getKeyNodes().get(i), scriptScope),
                injectCast(userMapInitNode.getValueNodes().get(i), scriptScope)
            );
        }

        scriptScope.putDecoration(userMapInitNode, new IRNodeDecoration(irMapInitializationNode));
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, ScriptScope scriptScope) {
        NewArrayNode irNewArrayNode = new NewArrayNode(userNewArrayNode.getLocation());

        irNewArrayNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userNewArrayNode, ValueType.class).valueType()));

        if (userNewArrayNode.isInitializer()) {
            irNewArrayNode.attachCondition(IRCInitialize.class);
        }

        for (AExpression userArgumentNode : userNewArrayNode.getValueNodes()) {
            irNewArrayNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewArrayNode, new IRNodeDecoration(irNewArrayNode));
    }

    @Override
    public void visitNewObj(ENewObj userNewObjectNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userNewObjectNode, ValueType.class).valueType();
        PainlessConstructor painlessConstructor = scriptScope.getDecoration(userNewObjectNode, StandardPainlessConstructor.class)
            .standardPainlessConstructor();

        NewObjectNode irNewObjectNode = new NewObjectNode(userNewObjectNode.getLocation());
        irNewObjectNode.attachDecoration(new IRDExpressionType(valueType));
        irNewObjectNode.attachDecoration(new IRDConstructor(painlessConstructor));

        if (scriptScope.getCondition(userNewObjectNode, Read.class)) {
            irNewObjectNode.attachCondition(IRCRead.class);
        }

        for (AExpression userArgumentNode : userNewObjectNode.getArgumentNodes()) {
            irNewObjectNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewObjectNode, new IRNodeDecoration(irNewObjectNode));
    }

    @Override
    public void visitCallLocal(ECallLocal callLocalNode, ScriptScope scriptScope) {
        InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(callLocalNode.getLocation());

        if (scriptScope.hasDecoration(callLocalNode, StandardLocalFunction.class)) {
            LocalFunction localFunction = scriptScope.getDecoration(callLocalNode, StandardLocalFunction.class).localFunction();
            irInvokeCallMemberNode.attachDecoration(new IRDFunction(localFunction));
        } else if (scriptScope.hasDecoration(callLocalNode, ThisPainlessMethod.class)) {
            PainlessMethod thisMethod = scriptScope.getDecoration(callLocalNode, ThisPainlessMethod.class).thisPainlessMethod();
            irInvokeCallMemberNode.attachDecoration(new IRDThisMethod(thisMethod));
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessMethod.class)) {
            PainlessMethod importedMethod = scriptScope.getDecoration(callLocalNode, StandardPainlessMethod.class).standardPainlessMethod();
            irInvokeCallMemberNode.attachDecoration(new IRDMethod(importedMethod));
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessClassBinding.class)) {
            PainlessClassBinding painlessClassBinding = scriptScope.getDecoration(callLocalNode, StandardPainlessClassBinding.class)
                .painlessClassBinding();
            String bindingName = scriptScope.getNextSyntheticName("class_binding");

            FieldNode irFieldNode = new FieldNode(callLocalNode.getLocation());
            irFieldNode.attachDecoration(new IRDModifiers(Modifier.PRIVATE));
            irFieldNode.attachDecoration(new IRDFieldType(painlessClassBinding.javaConstructor().getDeclaringClass()));
            irFieldNode.attachDecoration(new IRDName(bindingName));
            irClassNode.addFieldNode(irFieldNode);

            irInvokeCallMemberNode.attachDecoration(new IRDClassBinding(painlessClassBinding));

            if ((int) scriptScope.getDecoration(callLocalNode, StandardConstant.class).standardConstant() == 0) {
                irInvokeCallMemberNode.attachCondition(IRCStatic.class);
            }

            irInvokeCallMemberNode.attachDecoration(new IRDName(bindingName));
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessInstanceBinding.class)) {
            PainlessInstanceBinding painlessInstanceBinding = scriptScope.getDecoration(
                callLocalNode,
                StandardPainlessInstanceBinding.class
            ).painlessInstanceBinding();
            String bindingName = scriptScope.getNextSyntheticName("instance_binding");

            FieldNode irFieldNode = new FieldNode(callLocalNode.getLocation());
            irFieldNode.attachDecoration(new IRDModifiers(Modifier.PUBLIC | Modifier.STATIC));
            irFieldNode.attachDecoration(new IRDFieldType(painlessInstanceBinding.targetInstance().getClass()));
            irFieldNode.attachDecoration(new IRDName(bindingName));
            irClassNode.addFieldNode(irFieldNode);

            irInvokeCallMemberNode.attachDecoration(new IRDInstanceBinding(painlessInstanceBinding));
            irInvokeCallMemberNode.attachDecoration(new IRDName(bindingName));

            scriptScope.addStaticConstant(bindingName, painlessInstanceBinding.targetInstance());
        } else {
            throw callLocalNode.createError(new IllegalStateException("illegal tree structure"));
        }

        for (AExpression userArgumentNode : callLocalNode.getArgumentNodes()) {
            irInvokeCallMemberNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        Class<?> valueType = scriptScope.getDecoration(callLocalNode, ValueType.class).valueType();
        irInvokeCallMemberNode.attachDecoration(new IRDExpressionType(valueType));

        scriptScope.putDecoration(callLocalNode, new IRNodeDecoration(irInvokeCallMemberNode));
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userBooleanConstantNode, ValueType.class).valueType();
        Object constant = scriptScope.getDecoration(userBooleanConstantNode, StandardConstant.class).standardConstant();

        ConstantNode irConstantNode = new ConstantNode(userBooleanConstantNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.attachDecoration(new IRDConstant(constant));

        scriptScope.putDecoration(userBooleanConstantNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userNumericNode, ValueType.class).valueType();
        Object constant = scriptScope.getDecoration(userNumericNode, StandardConstant.class).standardConstant();

        ConstantNode irConstantNode = new ConstantNode(userNumericNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.attachDecoration(new IRDConstant(constant));

        scriptScope.putDecoration(userNumericNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userDecimalNode, ValueType.class).valueType();
        Object constant = scriptScope.getDecoration(userDecimalNode, StandardConstant.class).standardConstant();

        ConstantNode irConstantNode = new ConstantNode(userDecimalNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.attachDecoration(new IRDConstant(constant));

        scriptScope.putDecoration(userDecimalNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitString(EString userStringNode, ScriptScope scriptScope) {
        Class<?> valueType = scriptScope.getDecoration(userStringNode, ValueType.class).valueType();
        Object constant = scriptScope.getDecoration(userStringNode, StandardConstant.class).standardConstant();

        ConstantNode irConstantNode = new ConstantNode(userStringNode.getLocation());
        irConstantNode.attachDecoration(new IRDExpressionType(valueType));
        irConstantNode.attachDecoration(new IRDConstant(constant));

        scriptScope.putDecoration(userStringNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNull(ENull userNullNode, ScriptScope scriptScope) {
        NullNode irNullNode = new NullNode(userNullNode.getLocation());
        irNullNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userNullNode, ValueType.class).valueType()));

        scriptScope.putDecoration(userNullNode, new IRNodeDecoration(irNullNode));
    }

    @Override
    public void visitRegex(ERegex userRegexNode, ScriptScope scriptScope) {
        ConstantNode constant = new ConstantNode(userRegexNode.getLocation());
        constant.attachDecoration(new IRDExpressionType(Pattern.class));
        constant.attachDecoration(new IRDConstant(scriptScope.getDecoration(userRegexNode, StandardConstant.class).standardConstant()));
        scriptScope.putDecoration(userRegexNode, new IRNodeDecoration(constant));
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userLambdaNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(userLambdaNode.getLocation());
            typedInterfaceReferenceNode.attachDecoration(
                new IRDReference(scriptScope.getDecoration(userLambdaNode, ReferenceDecoration.class).reference())
            );
            irExpressionNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userLambdaNode.getLocation());
            defInterfaceReferenceNode.attachDecoration(
                new IRDDefReferenceEncoding(scriptScope.getDecoration(userLambdaNode, EncodingDecoration.class).encoding())
            );
            irExpressionNode = defInterfaceReferenceNode;
        }

        FunctionNode irFunctionNode = new FunctionNode(userLambdaNode.getLocation());
        irFunctionNode.setBlockNode((BlockNode) visit(userLambdaNode.getBlockNode(), scriptScope));
        irFunctionNode.attachDecoration(new IRDName(scriptScope.getDecoration(userLambdaNode, MethodNameDecoration.class).methodName()));
        irFunctionNode.attachDecoration(new IRDReturnType(scriptScope.getDecoration(userLambdaNode, ReturnType.class).returnType()));
        irFunctionNode.attachDecoration(
            new IRDTypeParameters(scriptScope.getDecoration(userLambdaNode, TypeParameters.class).typeParameters())
        );
        irFunctionNode.attachDecoration(
            new IRDParameterNames(scriptScope.getDecoration(userLambdaNode, ParameterNames.class).parameterNames())
        );
        if (scriptScope.getCondition(userLambdaNode, InstanceCapturingLambda.class)) {
            irFunctionNode.attachCondition(IRCInstanceCapture.class);
            irExpressionNode.attachCondition(IRCInstanceCapture.class);
        } else {
            irFunctionNode.attachCondition(IRCStatic.class);
        }
        irFunctionNode.attachCondition(IRCSynthetic.class);
        irFunctionNode.attachDecoration(new IRDMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter()));
        irClassNode.addFunctionNode(irFunctionNode);

        irExpressionNode.attachDecoration(new IRDExpressionType(scriptScope.getDecoration(userLambdaNode, ValueType.class).valueType()));

        List<Variable> captures = scriptScope.getDecoration(userLambdaNode, CapturesDecoration.class).captures();

        if (captures.isEmpty() == false) {
            List<String> captureNames = captures.stream().map(Variable::name).toList();
            irExpressionNode.attachDecoration(new IRDCaptureNames(captureNames));
        }

        scriptScope.putDecoration(userLambdaNode, new IRNodeDecoration(irExpressionNode));
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, ScriptScope scriptScope) {
        ExpressionNode irReferenceNode;

        TargetType targetType = scriptScope.getDecoration(userFunctionRefNode, TargetType.class);
        CapturesDecoration capturesDecoration = scriptScope.getDecoration(userFunctionRefNode, CapturesDecoration.class);

        if (targetType == null) {
            Def.Encoding encoding = scriptScope.getDecoration(userFunctionRefNode, EncodingDecoration.class).encoding();
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userFunctionRefNode.getLocation());
            defInterfaceReferenceNode.attachDecoration(new IRDDefReferenceEncoding(encoding));
            if (scriptScope.getCondition(userFunctionRefNode, InstanceCapturingFunctionRef.class)) {
                defInterfaceReferenceNode.attachCondition(IRCInstanceCapture.class);
            }
            irReferenceNode = defInterfaceReferenceNode;
        } else if (capturesDecoration != null && capturesDecoration.captures().get(0).type() == def.class) {
            TypedCaptureReferenceNode typedCaptureReferenceNode = new TypedCaptureReferenceNode(userFunctionRefNode.getLocation());
            typedCaptureReferenceNode.attachDecoration(new IRDName(userFunctionRefNode.getMethodName()));
            irReferenceNode = typedCaptureReferenceNode;
        } else {
            FunctionRef reference = scriptScope.getDecoration(userFunctionRefNode, ReferenceDecoration.class).reference();
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(userFunctionRefNode.getLocation());
            typedInterfaceReferenceNode.attachDecoration(new IRDReference(reference));
            if (scriptScope.getCondition(userFunctionRefNode, InstanceCapturingFunctionRef.class)) {
                typedInterfaceReferenceNode.attachCondition(IRCInstanceCapture.class);
            }
            irReferenceNode = typedInterfaceReferenceNode;
        }

        irReferenceNode.attachDecoration(
            new IRDExpressionType(scriptScope.getDecoration(userFunctionRefNode, ValueType.class).valueType())
        );

        if (capturesDecoration != null) {
            irReferenceNode.attachDecoration(new IRDCaptureNames(List.of(capturesDecoration.captures().get(0).name())));

            if (scriptScope.getCondition(userFunctionRefNode, CaptureBox.class)) {
                irReferenceNode.attachCondition(IRCCaptureBox.class);
            }
        }

        scriptScope.putDecoration(userFunctionRefNode, new IRNodeDecoration(irReferenceNode));
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scriptScope) {
        ExpressionNode irReferenceNode;

        if (scriptScope.hasDecoration(userNewArrayFunctionRefNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(
                userNewArrayFunctionRefNode.getLocation()
            );
            FunctionRef reference = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReferenceDecoration.class).reference();
            typedInterfaceReferenceNode.attachDecoration(new IRDReference(reference));
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            Def.Encoding encoding = scriptScope.getDecoration(userNewArrayFunctionRefNode, EncodingDecoration.class).encoding();
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userNewArrayFunctionRefNode.getLocation());
            defInterfaceReferenceNode.attachDecoration(new IRDDefReferenceEncoding(encoding));
            irReferenceNode = defInterfaceReferenceNode;
        }

        Class<?> returnType = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReturnType.class).returnType();

        LoadVariableNode irLoadVariableNode = new LoadVariableNode(userNewArrayFunctionRefNode.getLocation());
        irLoadVariableNode.attachDecoration(new IRDExpressionType(int.class));
        irLoadVariableNode.attachDecoration(new IRDName("size"));

        NewArrayNode irNewArrayNode = new NewArrayNode(userNewArrayFunctionRefNode.getLocation());
        irNewArrayNode.attachDecoration(new IRDExpressionType(returnType));
        irNewArrayNode.addArgumentNode(irLoadVariableNode);

        ReturnNode irReturnNode = new ReturnNode(userNewArrayFunctionRefNode.getLocation());
        irReturnNode.setExpressionNode(irNewArrayNode);

        BlockNode irBlockNode = new BlockNode(userNewArrayFunctionRefNode.getLocation());
        irBlockNode.attachCondition(IRCAllEscape.class);
        irBlockNode.addStatementNode(irReturnNode);

        FunctionNode irFunctionNode = new FunctionNode(userNewArrayFunctionRefNode.getLocation());
        irFunctionNode.attachDecoration(
            new IRDName(scriptScope.getDecoration(userNewArrayFunctionRefNode, MethodNameDecoration.class).methodName())
        );
        irFunctionNode.attachDecoration(new IRDReturnType(returnType));
        irFunctionNode.attachDecoration(new IRDTypeParameters(List.of(int.class)));
        irFunctionNode.attachDecoration(new IRDParameterNames(List.of("size")));
        irFunctionNode.attachCondition(IRCStatic.class);
        irFunctionNode.attachCondition(IRCSynthetic.class);
        irFunctionNode.attachDecoration(new IRDMaxLoopCounter(0));
        irFunctionNode.setBlockNode(irBlockNode);

        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.attachDecoration(
            new IRDExpressionType(scriptScope.getDecoration(userNewArrayFunctionRefNode, ValueType.class).valueType())
        );

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
            Class<?> staticType = scriptScope.getDecoration(userSymbolNode, StaticType.class).staticType();
            StaticNode staticNode = new StaticNode(userSymbolNode.getLocation());
            staticNode.attachDecoration(new IRDExpressionType(staticType));
            irExpressionNode = staticNode;
        } else if (scriptScope.hasDecoration(userSymbolNode, ValueType.class)) {
            boolean read = scriptScope.getCondition(userSymbolNode, Read.class);
            boolean write = scriptScope.getCondition(userSymbolNode, Write.class);
            boolean compound = scriptScope.getCondition(userSymbolNode, Compound.class);
            Location location = userSymbolNode.getLocation();
            String symbol = userSymbolNode.getSymbol();
            Class<?> valueType = scriptScope.getDecoration(userSymbolNode, ValueType.class).valueType();

            UnaryNode irStoreNode = null;
            ExpressionNode irLoadNode = null;

            if (write || compound) {
                StoreVariableNode irStoreVariableNode = new StoreVariableNode(location);
                irStoreVariableNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreVariableNode.attachDecoration(new IRDStoreType(valueType));
                irStoreVariableNode.attachDecoration(new IRDName(symbol));
                irStoreNode = irStoreVariableNode;
            }

            if (write == false || compound) {
                LoadVariableNode irLoadVariableNode = new LoadVariableNode(location);
                irLoadVariableNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadVariableNode.attachDecoration(new IRDName(symbol));
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
            Class<?> staticType = scriptScope.getDecoration(userDotNode, StaticType.class).staticType();
            StaticNode staticNode = new StaticNode(userDotNode.getLocation());
            staticNode.attachDecoration(new IRDExpressionType(staticType));
            irExpressionNode = staticNode;
        } else {
            boolean read = scriptScope.getCondition(userDotNode, Read.class);
            boolean write = scriptScope.getCondition(userDotNode, Write.class);
            boolean compound = scriptScope.getCondition(userDotNode, Compound.class);
            Location location = userDotNode.getLocation();
            String index = userDotNode.getIndex();
            Class<?> valueType = scriptScope.getDecoration(userDotNode, ValueType.class).valueType();
            ValueType prefixValueType = scriptScope.getDecoration(userDotNode.getPrefixNode(), ValueType.class);

            ExpressionNode irPrefixNode = (ExpressionNode) visit(userDotNode.getPrefixNode(), scriptScope);
            ExpressionNode irIndexNode = null;
            UnaryNode irStoreNode = null;
            ExpressionNode irLoadNode = null;
            int accessDepth;

            if (prefixValueType != null && prefixValueType.valueType().isArray()) {
                LoadDotArrayLengthNode irLoadDotArrayLengthNode = new LoadDotArrayLengthNode(location);
                irLoadDotArrayLengthNode.attachDecoration(new IRDExpressionType(int.class));
                irLoadNode = irLoadDotArrayLengthNode;

                accessDepth = 1;
            } else if (prefixValueType != null && prefixValueType.valueType() == def.class) {
                if (write || compound) {
                    StoreDotDefNode irStoreDotDefNode = new StoreDotDefNode(location);
                    irStoreDotDefNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreDotDefNode.attachDecoration(new IRDStoreType(valueType));
                    irStoreDotDefNode.attachDecoration(new IRDValue(index));
                    irStoreNode = irStoreDotDefNode;
                }

                if (write == false || compound) {
                    LoadDotDefNode irLoadDotDefNode = new LoadDotDefNode(location);
                    irLoadDotDefNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadDotDefNode.attachDecoration(new IRDValue(userDotNode.getIndex()));
                    irLoadNode = irLoadDotDefNode;
                }

                accessDepth = 1;
            } else if (scriptScope.hasDecoration(userDotNode, StandardPainlessField.class)) {
                PainlessField painlessField = scriptScope.getDecoration(userDotNode, StandardPainlessField.class).standardPainlessField();

                if (write || compound) {
                    StoreDotNode irStoreDotNode = new StoreDotNode(location);
                    irStoreDotNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreDotNode.attachDecoration(new IRDStoreType(valueType));
                    irStoreDotNode.attachDecoration(new IRDField(painlessField));
                    irStoreNode = irStoreDotNode;
                }

                if (write == false || compound) {
                    LoadDotNode irLoadDotNode = new LoadDotNode(location);
                    irLoadDotNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadDotNode.attachDecoration(new IRDField(painlessField));
                    irLoadNode = irLoadDotNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, Shortcut.class)) {
                if (write || compound) {
                    StoreDotShortcutNode irStoreDotShortcutNode = new StoreDotShortcutNode(location);
                    irStoreDotShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreDotShortcutNode.attachDecoration(new IRDStoreType(valueType));
                    irStoreDotShortcutNode.attachDecoration(
                        new IRDMethod(scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).setterPainlessMethod())
                    );
                    irStoreNode = irStoreDotShortcutNode;
                }

                if (write == false || compound) {
                    LoadDotShortcutNode irLoadDotShortcutNode = new LoadDotShortcutNode(location);
                    irLoadDotShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadDotShortcutNode.attachDecoration(
                        new IRDMethod(scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getterPainlessMethod())
                    );
                    irLoadNode = irLoadDotShortcutNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, MapShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode(location);
                irConstantNode.attachDecoration(new IRDExpressionType(String.class));
                irConstantNode.attachDecoration(new IRDConstant(index));
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode(location);
                    irStoreMapShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreMapShortcutNode.attachDecoration(new IRDStoreType(valueType));
                    irStoreMapShortcutNode.attachDecoration(
                        new IRDMethod(scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).setterPainlessMethod())
                    );
                    irStoreNode = irStoreMapShortcutNode;
                }

                if (write == false || compound) {
                    LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode(location);
                    irLoadMapShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadMapShortcutNode.attachDecoration(
                        new IRDMethod(scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getterPainlessMethod())
                    );
                    irLoadNode = irLoadMapShortcutNode;
                }

                accessDepth = 2;
            } else if (scriptScope.getCondition(userDotNode, ListShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode(location);
                irConstantNode.attachDecoration(new IRDExpressionType(int.class));
                irConstantNode.attachDecoration(
                    new IRDConstant(scriptScope.getDecoration(userDotNode, StandardConstant.class).standardConstant())
                );
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode(location);
                    irStoreListShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                    irStoreListShortcutNode.attachDecoration(new IRDStoreType(valueType));
                    irStoreListShortcutNode.attachDecoration(
                        new IRDMethod(scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).setterPainlessMethod())
                    );
                    irStoreNode = irStoreListShortcutNode;
                }

                if (write == false || compound) {
                    LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode(location);
                    irLoadListShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                    irLoadListShortcutNode.attachDecoration(
                        new IRDMethod(scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getterPainlessMethod())
                    );
                    irLoadNode = irLoadListShortcutNode;
                }

                accessDepth = 2;
            } else {
                throw userDotNode.createError(new IllegalStateException("illegal tree structure"));
            }

            scriptScope.putDecoration(userDotNode, new AccessDepth(accessDepth));
            irExpressionNode = buildLoadStore(
                accessDepth,
                location,
                userDotNode.isNullSafe(),
                irPrefixNode,
                irIndexNode,
                irLoadNode,
                irStoreNode
            );
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
        Class<?> valueType = scriptScope.getDecoration(userBraceNode, ValueType.class).valueType();
        Class<?> prefixValueType = scriptScope.getDecoration(userBraceNode.getPrefixNode(), ValueType.class).valueType();

        ExpressionNode irPrefixNode = (ExpressionNode) visit(userBraceNode.getPrefixNode(), scriptScope);
        ExpressionNode irIndexNode = injectCast(userBraceNode.getIndexNode(), scriptScope);
        UnaryNode irStoreNode = null;
        ExpressionNode irLoadNode = null;

        if (prefixValueType.isArray()) {
            FlipArrayIndexNode irFlipArrayIndexNode = new FlipArrayIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipArrayIndexNode.attachDecoration(new IRDExpressionType(int.class));
            irFlipArrayIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipArrayIndexNode;

            if (write || compound) {
                StoreBraceNode irStoreBraceNode = new StoreBraceNode(location);
                irStoreBraceNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreBraceNode.attachDecoration(new IRDStoreType(valueType));
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceNode irLoadBraceNode = new LoadBraceNode(location);
                irLoadBraceNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadNode = irLoadBraceNode;
            }
        } else if (prefixValueType == def.class) {
            Class<?> indexType = scriptScope.getDecoration(userBraceNode.getIndexNode(), ValueType.class).valueType();
            FlipDefIndexNode irFlipDefIndexNode = new FlipDefIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipDefIndexNode.attachDecoration(new IRDExpressionType(indexType));
            irFlipDefIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipDefIndexNode;

            if (write || compound) {
                StoreBraceDefNode irStoreBraceNode = new StoreBraceDefNode(location);
                irStoreBraceNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreBraceNode.attachDecoration(new IRDStoreType(valueType));
                irStoreBraceNode.attachDecoration(new IRDIndexType(indexType));
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceDefNode irLoadBraceDefNode = new LoadBraceDefNode(location);
                irLoadBraceDefNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadBraceDefNode.attachDecoration(new IRDIndexType(indexType));
                irLoadNode = irLoadBraceDefNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, MapShortcut.class)) {
            if (write || compound) {
                PainlessMethod setter = scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).setterPainlessMethod();
                StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode(location);
                irStoreMapShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreMapShortcutNode.attachDecoration(new IRDStoreType(valueType));
                irStoreMapShortcutNode.attachDecoration(new IRDMethod(setter));
                irStoreNode = irStoreMapShortcutNode;
            }

            if (write == false || compound) {
                PainlessMethod getter = scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getterPainlessMethod();
                LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode(location);
                irLoadMapShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadMapShortcutNode.attachDecoration(new IRDMethod(getter));
                irLoadNode = irLoadMapShortcutNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, ListShortcut.class)) {
            FlipCollectionIndexNode irFlipCollectionIndexNode = new FlipCollectionIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipCollectionIndexNode.attachDecoration(new IRDExpressionType(int.class));
            irFlipCollectionIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipCollectionIndexNode;

            if (write || compound) {
                PainlessMethod setter = scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).setterPainlessMethod();
                StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode(location);
                irStoreListShortcutNode.attachDecoration(new IRDExpressionType(read ? valueType : void.class));
                irStoreListShortcutNode.attachDecoration(new IRDStoreType(valueType));
                irStoreListShortcutNode.attachDecoration(new IRDMethod(setter));
                irStoreNode = irStoreListShortcutNode;
            }

            if (write == false || compound) {
                PainlessMethod getter = scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getterPainlessMethod();
                LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode(location);
                irLoadListShortcutNode.attachDecoration(new IRDExpressionType(valueType));
                irLoadListShortcutNode.attachDecoration(new IRDMethod(getter));
                irLoadNode = irLoadListShortcutNode;
            }
        } else {
            throw userBraceNode.createError(new IllegalStateException("illegal tree structure"));
        }

        scriptScope.putDecoration(userBraceNode, new AccessDepth(2));

        scriptScope.putDecoration(
            userBraceNode,
            new IRNodeDecoration(buildLoadStore(2, location, false, irPrefixNode, irIndexNode, irLoadNode, irStoreNode))
        );
    }

    @Override
    public void visitCall(ECall userCallNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        ValueType prefixValueType = scriptScope.getDecoration(userCallNode.getPrefixNode(), ValueType.class);
        Class<?> valueType = scriptScope.getDecoration(userCallNode, ValueType.class).valueType();

        if (scriptScope.getCondition(userCallNode, DynamicInvocation.class)) {
            InvokeCallDefNode irCallSubDefNode = new InvokeCallDefNode(userCallNode.getLocation());

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                irCallSubDefNode.addArgumentNode((ExpressionNode) visit(userArgumentNode, scriptScope));
            }

            irCallSubDefNode.attachDecoration(new IRDExpressionType(valueType));
            irCallSubDefNode.attachDecoration(new IRDName(userCallNode.getMethodName()));
            irExpressionNode = irCallSubDefNode;
        } else {
            Class<?> boxType;

            if (prefixValueType != null) {
                boxType = prefixValueType.valueType();
            } else {
                boxType = scriptScope.getDecoration(userCallNode.getPrefixNode(), StaticType.class).staticType();
            }

            InvokeCallNode irInvokeCallNode = new InvokeCallNode(userCallNode.getLocation());
            PainlessMethod method = scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).standardPainlessMethod();
            Object[] injections = PainlessLookupUtility.buildInjections(method, scriptScope.getCompilerSettings().asMap());
            Class<?>[] parameterTypes = method.javaMethod().getParameterTypes();
            int augmentedOffset = method.javaMethod().getDeclaringClass() == method.targetClass() ? 0 : 1;

            for (int i = 0; i < injections.length; i++) {
                Object injection = injections[i];
                Class<?> parameterType = parameterTypes[i + augmentedOffset];

                if (parameterType != PainlessLookupUtility.typeToUnboxedType(injection.getClass())) {
                    throw new IllegalStateException("illegal tree structure");
                }

                ConstantNode constantNode = new ConstantNode(userCallNode.getLocation());
                constantNode.attachDecoration(new IRDExpressionType(parameterType));
                constantNode.attachDecoration(new IRDConstant(injection));
                irInvokeCallNode.addArgumentNode(constantNode);
            }

            for (AExpression userCallArgumentNode : userCallNode.getArgumentNodes()) {
                irInvokeCallNode.addArgumentNode(injectCast(userCallArgumentNode, scriptScope));
            }

            irInvokeCallNode.attachDecoration(new IRDExpressionType(valueType));
            irInvokeCallNode.setMethod(scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).standardPainlessMethod());
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
        irBinaryImplNode.setLeftNode((ExpressionNode) visit(userCallNode.getPrefixNode(), scriptScope));
        irBinaryImplNode.setRightNode(irExpressionNode);
        irBinaryImplNode.attachDecoration(irExpressionNode.getDecoration(IRDExpressionType.class));

        scriptScope.putDecoration(userCallNode, new IRNodeDecoration(irBinaryImplNode));
    }
}
