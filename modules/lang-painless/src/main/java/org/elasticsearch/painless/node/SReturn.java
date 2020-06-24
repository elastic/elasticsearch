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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

/**
 * Represents a return statement.
 */
public class SReturn extends AStatement {

    private final AExpression expressionNode;

    public SReturn(int identifier, Location location, AExpression expressionNode) {
        super(identifier, location);

        this.expressionNode = expressionNode;
    }

    public AExpression getExpressionNode() {
        return expressionNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Output output = new Output();

        AExpression.Output expressionOutput = null;
        PainlessCast expressionCast = null;

        if (expressionNode == null) {
            if (semanticScope.getReturnType() != void.class) {
                throw getLocation().createError(new ClassCastException("Cannot cast from " +
                        "[" + semanticScope.getReturnCanonicalTypeName() + "] to " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]."));
            }
        } else {
            AExpression.Input expressionInput = new AExpression.Input();
            expressionInput.expected = semanticScope.getReturnType();
            expressionInput.internal = true;
            expressionOutput = AExpression.analyze(expressionNode, classNode, semanticScope, expressionInput);
            expressionCast = AnalyzerCaster.getLegalCast(expressionNode.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal);
        }

        output.methodEscape = true;
        output.loopEscape = true;
        output.allEscape = true;

        output.statementCount = 1;

        ReturnNode returnNode = new ReturnNode();
        returnNode.setExpressionNode(expressionNode == null ? null : AExpression.cast(expressionOutput.expressionNode, expressionCast));
        returnNode.setLocation(getLocation());

        output.statementNode = returnNode;

        return output;
    }
}
