/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;

public record EsqlQuery(LogicalPlan plan, List<QuerySettings> settings) {
    /**
     * Returns the expression corresponding to a setting value.
     * If the setting name appears multiple times (in one or more QuerySettings objects), this will return last occurrence.
     *
     * @param name the setting name
     */
    public Expression setting(String name) {
        if (settings == null) {
            return null;
        }
        Expression result = null;
        for (QuerySettings setting : settings) {
            for (Alias field : setting.fields()) {
                if (field.name().equals(name)) {
                    result = field.child();
                }
            }
        }
        return result;
    }
}
