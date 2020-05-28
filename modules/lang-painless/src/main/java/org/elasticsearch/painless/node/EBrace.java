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
import org.elasticsearch.painless.Scope;
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
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an array load/store and defers to a child subnode.
 */
public class EBrace extends AExpression {

    protected final AExpression prefix;
    protected final AExpression index;

    public EBrace(Location location, AExpression prefix, AExpression index) {
        super(location);

        this.prefix = Objects.requireNonNull(prefix);
        this.index = Objects.requireNonNull(index);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.read == false && input.write == false) {
            throw createError(new IllegalArgumentException("not a statement: result of brace operator not used"));
        }

        Output prefixOutput = analyze(prefix, classNode, scriptRoot, scope, new Input());

        ExpressionNode expressionNode;
        Output output = new Output();

        if (prefixOutput.actual.isArray()) {
            Input indexInput = new Input();
            indexInput.expected = int.class;
            Output indexOutput = analyze(index, classNode, scriptRoot, scope, indexInput);
            PainlessCast indexCast = AnalyzerCaster.getLegalCast(index.location,
                    indexOutput.actual, indexInput.expected, indexInput.explicit, indexInput.internal);

            output.actual = prefixOutput.actual.getComponentType();

            BraceSubNode braceSubNode = new BraceSubNode();
            braceSubNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            braceSubNode.setLocation(location);
            braceSubNode.setExpressionType(output.actual);
            expressionNode = braceSubNode;
        } else if (prefixOutput.actual == def.class) {
            Input indexInput = new Input();
            Output indexOutput = analyze(index, classNode, scriptRoot, scope, indexInput);

            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            output.actual = input.expected == null || input.expected == ZonedDateTime.class || input.explicit ? def.class : input.expected;
            output.isDefOptimized = true;

            BraceSubDefNode braceSubDefNode = new BraceSubDefNode();
            braceSubDefNode.setChildNode(indexOutput.expressionNode);
            braceSubDefNode.setLocation(location);
            braceSubDefNode.setExpressionType(output.actual);
            expressionNode = braceSubDefNode;
        } else if (Map.class.isAssignableFrom(prefixOutput.actual)) {
            Class<?> targetClass = prefixOutput.actual;
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);

            PainlessMethod getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetClass, false, "get", 1);
            PainlessMethod setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetClass, false, "put", 2);

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
                indexOutput = analyze(index, classNode, scriptRoot, scope, indexInput);
                indexCast = AnalyzerCaster.getLegalCast(index.location,
                        indexOutput.actual, indexInput.expected, indexInput.explicit, indexInput.internal);

                output.actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
            } else {
                throw createError(new IllegalArgumentException("Illegal map shortcut for type [" + canonicalClassName + "]."));
            }

            MapSubShortcutNode mapSubShortcutNode = new MapSubShortcutNode();
            mapSubShortcutNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            mapSubShortcutNode.setLocation(location);
            mapSubShortcutNode.setExpressionType(output.actual);
            mapSubShortcutNode.setGetter(getter);
            mapSubShortcutNode.setSetter(setter);
            expressionNode = mapSubShortcutNode;
        } else if (List.class.isAssignableFrom(prefixOutput.actual)) {
            Class<?> targetClass = prefixOutput.actual;
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);

            PainlessMethod getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetClass, false, "get", 1);
            PainlessMethod setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetClass, false, "set", 2);

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
                indexOutput = analyze(index, classNode, scriptRoot, scope, indexInput);
                indexCast = AnalyzerCaster.getLegalCast(index.location,
                        indexOutput.actual, indexInput.expected, indexInput.explicit, indexInput.internal);

                output.actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
            } else {
                throw createError(new IllegalArgumentException("Illegal list shortcut for type [" + canonicalClassName + "]."));
            }

            ListSubShortcutNode listSubShortcutNode = new ListSubShortcutNode();
            listSubShortcutNode.setChildNode(cast(indexOutput.expressionNode, indexCast));
            listSubShortcutNode.setLocation(location);
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
        braceNode.setLocation(location);
        braceNode.setExpressionType(output.actual);

        output.expressionNode = braceNode;

        return output;
    }
}
