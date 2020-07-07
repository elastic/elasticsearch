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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ir.BraceNode;
import org.elasticsearch.painless.ir.BraceSubDefNode;
import org.elasticsearch.painless.ir.BraceSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.ListSubShortcutNode;
import org.elasticsearch.painless.ir.MapSubShortcutNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.DefOptimized;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an array load/store and defers to a child subnode.
 */
public class EBrace extends AExpression {

    private final AExpression prefixNode;
    private final AExpression indexNode;

    public EBrace(int identifier, Location location, AExpression prefixNode, AExpression indexNode) {
        super(identifier, location);

        this.prefixNode = Objects.requireNonNull(prefixNode);
        this.indexNode = Objects.requireNonNull(indexNode);
    }

    public AExpression getPrefixNode() {
        return prefixNode;
    }

    public AExpression getIndexNode() {
        return indexNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitBrace(this, input);
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        boolean read = semanticScope.getCondition(this, Read.class);
        boolean write = semanticScope.getCondition(this, Write.class);

        if (read == false && write == false) {
            throw createError(new IllegalArgumentException("not a statement: result of brace operator not used"));
        }

        semanticScope.setCondition(prefixNode, Read.class);
        Output prefixOutput = analyze(prefixNode, classNode, semanticScope);
        Class<?> prefixValueType = semanticScope.getDecoration(prefixNode, ValueType.class).getValueType();

        ExpressionNode expressionNode;
        Output output = new Output();
        Class<?> valueType;

        if (prefixValueType.isArray()) {
            semanticScope.setCondition(indexNode, Read.class);
            semanticScope.putDecoration(indexNode, new TargetType(int.class));
            Output indexOutput = analyze(indexNode, classNode, semanticScope);
            PainlessCast indexCast = indexNode.cast(semanticScope);
            valueType = prefixValueType.getComponentType();

            BraceSubNode braceSubNode = new BraceSubNode();
            braceSubNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            braceSubNode.setLocation(getLocation());
            braceSubNode.setExpressionType(valueType);
            expressionNode = braceSubNode;
        } else if (prefixValueType == def.class) {
            semanticScope.setCondition(indexNode, Read.class);
            Output indexOutput = analyze(indexNode, classNode, semanticScope);

            TargetType targetType = semanticScope.getDecoration(this, TargetType.class);
            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                    semanticScope.getCondition(this, Explicit.class) ? def.class : targetType.getTargetType();
            semanticScope.setCondition(this, DefOptimized.class);

            BraceSubDefNode braceSubDefNode = new BraceSubDefNode();
            braceSubDefNode.setChildNode(indexOutput.expressionNode);
            braceSubDefNode.setLocation(getLocation());
            braceSubDefNode.setExpressionType(valueType);
            expressionNode = braceSubDefNode;
        } else if (Map.class.isAssignableFrom(prefixValueType)) {
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType);

            PainlessMethod getter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "get", 1);
            PainlessMethod setter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "put", 2);

            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1)) {
                throw createError(new IllegalArgumentException("Illegal map get shortcut for type [" + canonicalClassName + "]."));
            }

            if (setter != null && setter.typeParameters.size() != 2) {
                throw createError(new IllegalArgumentException("Illegal map set shortcut for type [" + canonicalClassName + "]."));
            }

            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0)) ||
                    !getter.returnType.equals(setter.typeParameters.get(1)))) {
                throw createError(new IllegalArgumentException("Shortcut argument types must match."));
            }

            Output indexOutput;
            PainlessCast indexCast;

            if ((read == false || getter != null) && (write == false || setter != null)) {
                semanticScope.setCondition(indexNode, Read.class);
                semanticScope.putDecoration(indexNode,
                        new TargetType(setter != null ? setter.typeParameters.get(0) : getter.typeParameters.get(0)));
                indexOutput = analyze(indexNode, classNode, semanticScope);
                indexCast = indexNode.cast(semanticScope);
                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;
            } else {
                throw createError(new IllegalArgumentException("Illegal map shortcut for type [" + canonicalClassName + "]."));
            }

            MapSubShortcutNode mapSubShortcutNode = new MapSubShortcutNode();
            mapSubShortcutNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            mapSubShortcutNode.setLocation(getLocation());
            mapSubShortcutNode.setExpressionType(valueType);
            mapSubShortcutNode.setGetter(getter);
            mapSubShortcutNode.setSetter(setter);
            expressionNode = mapSubShortcutNode;
        } else if (List.class.isAssignableFrom(prefixValueType)) {
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType);

            PainlessMethod getter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "get", 1);
            PainlessMethod setter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "set", 2);

            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1 ||
                    getter.typeParameters.get(0) != int.class)) {
                throw createError(new IllegalArgumentException("Illegal list get shortcut for type [" + canonicalClassName + "]."));
            }

            if (setter != null && (setter.typeParameters.size() != 2 || setter.typeParameters.get(0) != int.class)) {
                throw createError(new IllegalArgumentException("Illegal list set shortcut for type [" + canonicalClassName + "]."));
            }

            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0))
                    || !getter.returnType.equals(setter.typeParameters.get(1)))) {
                throw createError(new IllegalArgumentException("Shortcut argument types must match."));
            }

            Output indexOutput;
            PainlessCast indexCast;

            if ((read == false || getter != null) && (write == false || setter != null)) {
                semanticScope.setCondition(indexNode, Read.class);
                semanticScope.putDecoration(indexNode, new TargetType(int.class));
                indexOutput = analyze(indexNode, classNode, semanticScope);
                indexCast = indexNode.cast(semanticScope);
                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;
            } else {
                throw createError(new IllegalArgumentException("Illegal list shortcut for type [" + canonicalClassName + "]."));
            }

            ListSubShortcutNode listSubShortcutNode = new ListSubShortcutNode();
            listSubShortcutNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            listSubShortcutNode.setLocation(getLocation());
            listSubShortcutNode.setExpressionType(valueType);
            listSubShortcutNode.setGetter(getter);
            listSubShortcutNode.setSetter(setter);
            expressionNode = listSubShortcutNode;
        } else {
            throw createError(new IllegalArgumentException("Illegal array access on type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType) + "]."));
        }

        semanticScope.putDecoration(this, new ValueType(valueType));

        BraceNode braceNode = new BraceNode();
        braceNode.setLeftNode(prefixOutput.expressionNode);
        braceNode.setRightNode(expressionNode);
        braceNode.setLocation(getLocation());
        braceNode.setExpressionType(valueType);
        output.expressionNode = braceNode;

        return output;
    }
}
