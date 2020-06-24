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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.symbol.SemanticScope;
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
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.read == false && input.write == false) {
            throw createError(new IllegalArgumentException("not a statement: result of brace operator not used"));
        }

        Output prefixOutput = analyze(prefixNode, classNode, semanticScope, new Input());

        ExpressionNode expressionNode;
        Output output = new Output();

        if (prefixOutput.actual.isArray()) {
            Input indexInput = new Input();
            indexInput.expected = int.class;
            Output indexOutput = analyze(indexNode, classNode, semanticScope, indexInput);
            PainlessCast indexCast = AnalyzerCaster.getLegalCast(indexNode.getLocation(),
                    indexOutput.actual, indexInput.expected, indexInput.explicit, indexInput.internal);

            output.actual = prefixOutput.actual.getComponentType();

            BraceSubNode braceSubNode = new BraceSubNode();
            braceSubNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            braceSubNode.setLocation(getLocation());
            braceSubNode.setExpressionType(output.actual);
            expressionNode = braceSubNode;
        } else if (prefixOutput.actual == def.class) {
            Input indexInput = new Input();
            Output indexOutput = analyze(indexNode, classNode, semanticScope, indexInput);

            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            output.actual = input.expected == null || input.expected == ZonedDateTime.class || input.explicit ? def.class : input.expected;
            output.isDefOptimized = true;

            BraceSubDefNode braceSubDefNode = new BraceSubDefNode();
            braceSubDefNode.setChildNode(indexOutput.expressionNode);
            braceSubDefNode.setLocation(getLocation());
            braceSubDefNode.setExpressionType(output.actual);
            expressionNode = braceSubDefNode;
        } else if (Map.class.isAssignableFrom(prefixOutput.actual)) {
            Class<?> targetClass = prefixOutput.actual;
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);

            PainlessMethod getter = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(targetClass, false, "get", 1);
            PainlessMethod setter = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(targetClass, false, "put", 2);

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

            if ((input.read == false || getter != null) && (input.write == false || setter != null)) {
                Input indexInput = new Input();
                indexInput.expected = setter != null ? setter.typeParameters.get(0) : getter.typeParameters.get(0);
                indexOutput = analyze(indexNode, classNode, semanticScope, indexInput);
                indexCast = AnalyzerCaster.getLegalCast(indexNode.getLocation(),
                        indexOutput.actual, indexInput.expected, indexInput.explicit, indexInput.internal);

                output.actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
            } else {
                throw createError(new IllegalArgumentException("Illegal map shortcut for type [" + canonicalClassName + "]."));
            }

            MapSubShortcutNode mapSubShortcutNode = new MapSubShortcutNode();
            mapSubShortcutNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            mapSubShortcutNode.setLocation(getLocation());
            mapSubShortcutNode.setExpressionType(output.actual);
            mapSubShortcutNode.setGetter(getter);
            mapSubShortcutNode.setSetter(setter);
            expressionNode = mapSubShortcutNode;
        } else if (List.class.isAssignableFrom(prefixOutput.actual)) {
            Class<?> targetClass = prefixOutput.actual;
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);

            PainlessMethod getter = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(targetClass, false, "get", 1);
            PainlessMethod setter = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(targetClass, false, "set", 2);

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

            if ((input.read == false || getter != null) && (input.write == false || setter != null)) {
                Input indexInput = new Input();
                indexInput.expected = int.class;
                indexOutput = analyze(indexNode, classNode, semanticScope, indexInput);
                indexCast = AnalyzerCaster.getLegalCast(indexNode.getLocation(),
                        indexOutput.actual, indexInput.expected, indexInput.explicit, indexInput.internal);

                output.actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
            } else {
                throw createError(new IllegalArgumentException("Illegal list shortcut for type [" + canonicalClassName + "]."));
            }

            ListSubShortcutNode listSubShortcutNode = new ListSubShortcutNode();
            listSubShortcutNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            listSubShortcutNode.setLocation(getLocation());
            listSubShortcutNode.setExpressionType(output.actual);
            listSubShortcutNode.setGetter(getter);
            listSubShortcutNode.setSetter(setter);
            expressionNode = listSubShortcutNode;
        } else {
            throw createError(new IllegalArgumentException("Illegal array access on type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(prefixOutput.actual) + "]."));
        }

        BraceNode braceNode = new BraceNode();
        braceNode.setLeftNode(prefixOutput.expressionNode);
        braceNode.setRightNode(expressionNode);
        braceNode.setLocation(getLocation());
        braceNode.setExpressionType(output.actual);

        output.expressionNode = braceNode;

        return output;
    }
}
