/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

public record EsqlStatement(LogicalPlan plan, List<QuerySetting> settings) {
    /**
     * Returns the expression corresponding to a setting value.
     * If the setting name appears multiple times, this will return last occurrence.
     *
     * @param name the setting name
     */
    public Expression setting(String name) {
        if (settings == null) {
            return null;
        }
        Expression result = null;
        for (QuerySetting setting : settings) {
            if (setting.name().equals(name)) {
                result = setting.value();
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "EsqlStatement{" + "plan=" + plan + ", settings=" + settings + "}";
    }
}
