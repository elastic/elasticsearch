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

package org.elasticsearch.ingest.processor.conditional;

import org.elasticsearch.ingest.IngestDocument;

public class RelationalExpression implements ConditionalExpression {
    private final RelationalOperator operator;
    private final String field;
    private final Object operand;

    public RelationalExpression(String field, String operatorName, Object operand) {
        this(field, RelationalOperator.fromString(operatorName), operand);
    }

    public RelationalExpression(String field, RelationalOperator operator, Object operand) {
        this.field = field;
        this.operator = operator;
        this.operand = operand;
    }

    public RelationalOperator getOperator() {
        return operator;
    }

    public Object getOperand() {
        return operand;
    }

    public boolean matches(IngestDocument ingestDocument) {
        Object other = ingestDocument.getFieldValue(field, Object.class);
        if (operator.equals(RelationalOperator.EQUALS)) {
            if (operand == null) {
                return other == null;
            }
            return operand.equals(other);
        } else if (operator.equals(RelationalOperator.NOT_EQUALS)) {
            if (operand == null) {
                return other != null;
            }
            return !operand.equals(other);
        }


        // other operators require comparison between numbers.
        // comparisons cannot be made with null values.
        if (operand != null && other != null
                && (operand instanceof Number) && (other instanceof Number)) {
            // cast to double here for simplicity, needs cleaning up
            double left = ((Number) other).doubleValue();
            double right = ((Number) operand).doubleValue();
            switch (operator) {
                case LESS_THAN:
                    return left < right;
                case LESS_THAN_OR_EQUALS:
                    return left <= right;
                case GREATER_THAN:
                    return left > right;
                case GREATER_THAN_OR_EQUALS:
                    return left >= right;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "RelationalExpression[relation: " + operator.name + ", operand: " + operand;
    }

    enum RelationalOperator {
        EQUALS("eq"),
        NOT_EQUALS("neq"),
        GREATER_THAN_OR_EQUALS("gte"),
        LESS_THAN_OR_EQUALS("lte"),
        GREATER_THAN("gt"),
        LESS_THAN("lt");

        private String name;

        RelationalOperator(String name) {
            this.name = name;
        }

        public static RelationalOperator fromString(String text) {
            if (text != null) {
                for (RelationalOperator op : RelationalOperator.values()) {
                    if (text.equalsIgnoreCase(op.name)) {
                        return op;
                    }
                }
            }
            throw new IllegalArgumentException("String [" + text + "] does not match an existing operator constant.");
        }
    }
}
