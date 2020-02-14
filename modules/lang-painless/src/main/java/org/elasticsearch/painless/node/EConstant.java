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
import org.elasticsearch.painless.symbol.ScriptRoot;

/**
 * Represents a constant inserted into the tree replacing
 * other constants during constant folding.  (Internal only.)
 */
final class EConstant extends AExpression {

    EConstant(Location location, Object constant) {
        super(location);

        this.constant = constant;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        if (constant instanceof String) {
            actual = String.class;
        } else if (constant instanceof Double) {
            actual = double.class;
        } else if (constant instanceof Float) {
            actual = float.class;
        } else if (constant instanceof Long) {
            actual = long.class;
        } else if (constant instanceof Integer) {
            actual = int.class;
        } else if (constant instanceof Character) {
            actual = char.class;
        } else if (constant instanceof Short) {
            actual = short.class;
        } else if (constant instanceof Byte) {
            actual = byte.class;
        } else if (constant instanceof Boolean) {
            actual = boolean.class;
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    ConstantNode write(ClassNode classNode) {
        ConstantNode constantNode = new ConstantNode();

        constantNode.setLocation(location);
        constantNode.setExpressionType(actual);
        constantNode.setConstant(constant);

        return constantNode;
    }

    @Override
    public String toString() {
        String c = constant.toString();
        if (constant instanceof String) {
            c = "'" + c + "'";
        }
        return singleLineToString(constant.getClass().getSimpleName(), c);
    }
}
