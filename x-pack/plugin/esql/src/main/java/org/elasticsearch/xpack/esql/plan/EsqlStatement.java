/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

public record EsqlStatement(LogicalPlan plan, List<QuerySetting> settings) {
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
    public <T> T setting(QuerySettings.QuerySettingDef<T> settingDef) {
        return settingOrDefault(settingDef, settingDef.defaultValue());
    }

    /**
     * Returns the value of a setting, but returns the given default value if the setting is not set.
     * <p>
     *     Use it like:
     * </p>
     * <pre><code>
     *     var value = statement.settingOrDefault(QuerySettings.MY_SETTING, "default");
     * </code></pre>
     * <p>
     *     To be used when a fallback is available.
     * </p>
     *
     * @param settingDef the setting to retrieve
     * @param defaultValue the value to return if the setting is not set
     */
    public <T> T settingOrDefault(QuerySettings.QuerySettingDef<T> settingDef, T defaultValue) {
        Expression expression = setting(settingDef.name());
        if (expression == null) {
            return defaultValue;
        }
        return settingDef.parse((Literal) expression);
    }

    /**
     * Returns the expression corresponding to a setting value.
     * If the setting name appears multiple times, this will return last occurrence.
     * <p>
     *     For testing purposes; use {@link #setting(QuerySettings.QuerySettingDef)} instead.
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
