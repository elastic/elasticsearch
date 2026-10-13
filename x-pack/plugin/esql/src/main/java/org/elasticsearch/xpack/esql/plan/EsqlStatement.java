/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

public record EsqlStatement(LogicalPlan plan, List<QuerySetting> settings, PipeDepths pipeDepths) {

    /**
     * Number of pipes (processing commands) on the path from a leaf of a parsed statement up to its root, as recorded by the parser.
     * View resolution adds these up across view boundaries to enforce the maximum query depth on the fully expanded query, which no
     * single parse can see.
     *
     * @param max the largest number of pipes above any leaf of the statement
     * @param relations the number of pipes above each relation, keyed by the relation's {@link Source}. This includes the commands of
     *                  enclosing subqueries and {@code FORK} branches, and the command holding an {@code IN} subquery.
     */
    public record PipeDepths(int max, Map<Source, Integer> relations) {
        public static final PipeDepths EMPTY = new PipeDepths(0, Map.of());
    }

    public EsqlStatement(LogicalPlan plan, List<QuerySetting> settings) {
        this(plan, settings, PipeDepths.EMPTY);
    }

    /**
     * Returns the value of a setting, or the setting default value if the setting is not set.
     * If the setting name appears multiple times, this will return last occurrence.
     * <p>
     *     Use it like:
     * </p>
     * <pre><code>
     *     var value = statement.setting(QuerySettings.MY_SETTING);
     * </code></pre>
     *
     * @param settingDef the setting to retrieve
     */
    public <T> T setting(QuerySettingDef<T> settingDef) {
        return settingOrDefault(settingDef, settingDef.defaultValue());
    }

    /** Returns the SET value of a setting, or {@code defaultValue} if it was not set in the query. */
    private <T> T settingOrDefault(QuerySettingDef<T> settingDef, T defaultValue) {
        Expression expression = setting(settingDef.name());
        if (expression == null) {
            return defaultValue;
        }
        return settingDef.readFromExpression(expression);
    }

    /**
     * Returns the raw SET expression for a setting name, or {@code null} if not set.
     * If the setting name appears multiple times, this returns the last occurrence.
     * <p>
     *     This is the low-level accessor used by {@link QuerySettings#resolve} to fold SET values; for a typed,
     *     defaulted read prefer {@link #setting(QuerySettingDef)}.
     * </p>
     *
     * @param name the setting name
     */
    public Expression setting(String name) {
        if (settings == null) {
            return null;
        }
        Expression expression = null;
        for (QuerySetting setting : settings) {
            if (setting.name().equals(name)) {
                expression = setting.value();
            }
        }
        return expression;
    }

    @Override
    public String toString() {
        return "EsqlStatement{" + "plan=" + plan + ", settings=" + settings + "}";
    }
}
