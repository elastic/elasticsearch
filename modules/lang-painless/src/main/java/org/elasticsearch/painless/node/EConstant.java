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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

/**
 * Represents a constant inserted into the tree replacing
 * other constants during constant folding.  (Internal only.)
 */
public class EConstant extends AExpression {

    protected Object constant;

    public EConstant(Location location, Object constant) {
        super(location);

        this.constant = constant;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to constant [" + constant + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: constant [" + constant + "] not used"));
        }

        Output output = new Output();

        if (constant instanceof String) {
            output.actual = String.class;
        } else if (constant instanceof Double) {
            output.actual = double.class;
        } else if (constant instanceof Float) {
            output.actual = float.class;
        } else if (constant instanceof Long) {
            output.actual = long.class;
        } else if (constant instanceof Integer) {
            output.actual = int.class;
        } else if (constant instanceof Character) {
            output.actual = char.class;
        } else if (constant instanceof Short) {
            output.actual = short.class;
        } else if (constant instanceof Byte) {
            output.actual = byte.class;
        } else if (constant instanceof Boolean) {
            output.actual = boolean.class;
        } else {
            throw createError(new IllegalStateException("unexpected type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(constant.getClass()) + "] " +
                    "for constant node"));
        }

        ConstantNode constantNode = new ConstantNode();
        constantNode.setLocation(location);
        constantNode.setExpressionType(output.actual);
        constantNode.setConstant(constant);

        output.expressionNode = constantNode;

        return output;
    }
}
