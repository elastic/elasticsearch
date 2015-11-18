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

import java.util.List;

public final class BooleanExpression implements ConditionalExpression {

    private final BooleanOperator booleanOperator;
    private final List<RelationalExpression> relationalExpressions;

    public BooleanExpression(String booleanOperator, List<RelationalExpression> relationalExpressions) {
        this(BooleanOperator.fromString(booleanOperator), relationalExpressions);
    }

    public BooleanExpression(BooleanOperator booleanOperator, List<RelationalExpression> relationalExpressions) {
        this.booleanOperator = booleanOperator;
        this.relationalExpressions = relationalExpressions;
    }

    public BooleanOperator getBooleanOperator() {
        return booleanOperator;
    }

    public List<RelationalExpression> getRelationalExpressions() {
        return relationalExpressions;
    }

    public boolean matches(IngestDocument ingestDocument) {
        switch(booleanOperator) {
            case ALL:
                return matchesAll(ingestDocument);
            case ANY:
                return matchesAny(ingestDocument);
            default:
                return false;
        }
    }

    private boolean matchesAll(IngestDocument ingestDocument) {
        for (RelationalExpression e : relationalExpressions) {
            if (!e.matches(ingestDocument)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesAny(IngestDocument ingestDocument) {
        for (RelationalExpression e : relationalExpressions) {
            if (e.matches(ingestDocument)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "BooleanExpression[boolean_operator: " + booleanOperator.name + ", relationalExpressions:" + relationalExpressions + "]";
    }

    enum BooleanOperator {
        ALL("all"), ANY("any");

        private String name;

        BooleanOperator(String name) {
            this.name = name;
        }

        static BooleanOperator fromString(String text) {
            if (text != null) {
                for (BooleanOperator op : BooleanOperator.values()) {
                    if (text.equalsIgnoreCase(op.name)) {
                        return op;
                    }
                }
            }
            throw new IllegalArgumentException("String [" + text + "] does not match an existing operator constant.");
        }
    }
}

