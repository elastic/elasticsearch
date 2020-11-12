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

import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;

public class DefaultStringConcatenationOptimizationPhase extends IRTreeBaseVisitor<Void> {

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Void scope) {
         int i = 0;

        while (i < irStringConcatenationNode.getArgumentNodes().size()) {
            ExpressionNode irArgumentNode = irStringConcatenationNode.getArgumentNodes().get(i);

            if (irArgumentNode instanceof StringConcatenationNode) {
                irStringConcatenationNode.getArgumentNodes().remove(i);
                irStringConcatenationNode.getArgumentNodes().addAll(i, ((StringConcatenationNode)irArgumentNode).getArgumentNodes());
            } else {
                i++;
            }
        }

        for (ExpressionNode argumentNode : irStringConcatenationNode.getArgumentNodes()) {
            argumentNode.visit(this, scope);
        }
    }
}
