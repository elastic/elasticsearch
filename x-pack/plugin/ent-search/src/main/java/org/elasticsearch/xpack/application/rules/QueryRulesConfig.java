/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;

import java.util.List;

public class QueryRulesConfig {

    static final String SETTING_ROOT_PATH = "xpack.applications.rules";

    private static final int DEFAULT_RULE_LIMIT = 100;
    private static final int MIN_RULE_LIMIT = 1;
    private static final int MAX_RULE_LIMIT = 1000;

    /**
     * Index setting describing the maximum number of {@link QueryRule}s that can be included
     * in a query ruleset.
     */
    public static final Setting<Integer> MAX_RULE_LIMIT_SETTING = Setting.intSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "max_rules_per_ruleset"),
        DEFAULT_RULE_LIMIT,
        MIN_RULE_LIMIT,
        MAX_RULE_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettings() {
        return List.of(MAX_RULE_LIMIT_SETTING);
    }

    private QueryRulesConfig() {}

}
