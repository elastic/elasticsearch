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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.SemanticScope;

/**
 * Represents a return statement.
 */
public class SReturn extends AStatement {

    private final AExpression valueNode;

    public SReturn(int identifier, Location location, AExpression valueNode) {
        super(identifier, location);

        this.valueNode = valueNode;
    }

    public AExpression getValueNode() {
        return valueNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitReturn(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SReturn userReturnNode, SemanticScope semanticScope) {

        AExpression userValueNode = userReturnNode.getValueNode();

        if (userValueNode == null) {
            if (semanticScope.getReturnType() != void.class) {
                throw userReturnNode.createError(new ClassCastException("cannot cast from " +
                        "[" + semanticScope.getReturnCanonicalTypeName() + "] to " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]"));
            }
        } else {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(semanticScope.getReturnType()));
            semanticScope.setCondition(userValueNode, Internal.class);
            visitor.checkedVisit(userValueNode, semanticScope);
            visitor.decorateWithCast(userValueNode, semanticScope);
        }

        semanticScope.setCondition(userReturnNode, MethodEscape.class);
        semanticScope.setCondition(userReturnNode, LoopEscape.class);
        semanticScope.setCondition(userReturnNode, AllEscape.class);
    }
}
