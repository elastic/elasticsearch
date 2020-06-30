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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;

import java.util.Objects;

/**
 * Represents a decimal constant.
 */
public class EDecimal extends AExpression {

    private final String decimal;

    public EDecimal(int identifier, Location location, String decimal) {
        super(identifier, location);

        this.decimal = Objects.requireNonNull(decimal);
    }

    public String getDecimal() {
        return decimal;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        return analyze(input, false);
    }

    Output analyze(Input input, boolean negate) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to decimal constant [" + decimal + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: decimal constant [" + decimal + "] not used"));
        }

        Output output = new Output();
        Object constant;

        String decimal = negate ? "-" + this.decimal : this.decimal;

        if (decimal.endsWith("f") || decimal.endsWith("F")) {
            try {
                constant = Float.parseFloat(decimal.substring(0, decimal.length() - 1));
                output.actual = float.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid float constant [" + decimal + "]."));
            }
        } else {
            String toParse = decimal;
            if (toParse.endsWith("d") || decimal.endsWith("D")) {
                toParse = toParse.substring(0, decimal.length() - 1);
            }
            try {
                constant = Double.parseDouble(toParse);
                output.actual = double.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid double constant [" + decimal + "]."));
            }
        }

        ConstantNode constantNode = new ConstantNode();
        constantNode.setLocation(getLocation());
        constantNode.setExpressionType(output.actual);
        constantNode.setConstant(constant);

        output.expressionNode = constantNode;

        return output;
    }
}
