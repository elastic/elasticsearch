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
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.SemanticScope;

/**
 * The superclass for all E* (expression) and P* (postfix) nodes.
 */
public abstract class AExpression extends ANode {

//    public static class Input {
//
//        /**
//         * Set to false when an expression will not be read from such as
//         * a basic assignment.  Note this variable is always set by the parent
//         * as input.
//         */
//        boolean read = true;
//
//        /**
//         * Set to true when this node is an lhs-expression and will be storing
//         * a value from an rhs-expression.
//         */
//        boolean write = false;
//
//        /**
//         * Set to the expected type this node needs to be.  Note this variable
//         * is always set by the parent as input and should never be read from.
//         */
//        Class<?> expected = null;
//
//        /**
//         * Set by {@link EExplicit} if a cast made on an expression node should be
//         * explicit.
//         */
//        boolean explicit = false;
//
//        /**
//         * Set to true if a cast is allowed to boxed/unboxed.  This is used
//         * for method arguments because casting may be required.
//         */
//        boolean internal = false;
//    }
//
//    public static class Output {
//
//        /**
//         * Set to {@code true} when actual represents a static type and
//         * this expression does not generate a value. Set to {@code false}
//         * when actual is the type of value this expression generates.
//         */
//        boolean isStaticType = false;
//
//        /**
//         * Used to build a fully-qualified type name when the name comes
//         * in as pieces since x.y.z may get broken down into multiples nodes
//         * with the dot as a delimiter.
//         */
//        String partialCanonicalTypeName = null;
//
//        /**
//         * {@code true} if this node or a sub-node of this node can be optimized with
//         * rhs actual type to avoid an unnecessary cast.
//         */
//        boolean isDefOptimized = false;
//
//        /**
//         * The {@link ExpressionNode}(s) generated from this expression.
//         */
//        ExpressionNode expressionNode = null;
//    }

    /**
     * Standard constructor with location used for error tracking.
     */
    AExpression(int identifier, Location location) {
        super(identifier, location);
    }

    /**
     * Checks for errors and collects data for the writing phase.
     */
    void analyze(SemanticScope semanticScope) {
        throw new UnsupportedOperationException();
    }

    /**
     * Checks for errors and collects data for the writing phase. Adds additional, common
     * error checking for conditions related to static types and partially constructed static types.
     */
    static void analyze(AExpression expression, SemanticScope semanticScope) {
        expression.analyze(semanticScope);

        if (semanticScope.hasDecoration(expression, PartialCanonicalTypeName.class)) {
            throw expression.createError(new IllegalArgumentException("cannot resolve symbol " +
                    "[" + semanticScope.getDecoration(expression, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(expression, StaticType.class)) {
            throw expression.createError(new IllegalArgumentException("value required: instead found unexpected type " +
                    "[" + semanticScope.getDecoration(expression, StaticType.class).getStaticCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(expression, ValueType.class) == false) {
            throw expression.createError(new IllegalStateException("value required: instead found no value"));
        }
    }

    // TODO: move this somewhere more appropriate
    public PainlessCast cast(SemanticScope semanticScope) {
        Class<?> valueType = semanticScope.getDecoration(this, ValueType.class).getValueType();
        Class<?> targetType = semanticScope.getDecoration(this, TargetType.class).getTargetType();
        boolean isExplicitCast = semanticScope.getCondition(this, Explicit.class);
        boolean isInternalCast = semanticScope.getCondition(this, Internal.class);

        PainlessCast painlessCast = AnalyzerCaster.getLegalCast(getLocation(), valueType, targetType, isExplicitCast, isInternalCast);
        semanticScope.putDecoration(this, new ExpressionPainlessCast(painlessCast));

        return painlessCast;
    }
}
