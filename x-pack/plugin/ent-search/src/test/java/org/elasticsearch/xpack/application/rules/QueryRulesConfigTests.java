/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class QueryRulesConfigTests extends ESTestCase {

    public void testDefaultConfig() {
        int maxRules = QueryRulesConfig.MAX_RULE_LIMIT_SETTING.get(Settings.EMPTY);
        assertThat(maxRules, equalTo(100));
    }

    public void testCustomQueryRulesConfigMaxRulesPerRuleset() {
        int customMaxRules = randomIntBetween(1, 1000);
        Settings settings = createCustomSettings("max_rules_per_ruleset", Integer.toString(customMaxRules));
        int maxRules = QueryRulesConfig.MAX_RULE_LIMIT_SETTING.get(settings);
        assertThat(customMaxRules, equalTo(maxRules));
    }

    public void testCustomQueryRulesConfigMaxRulesPerRulesetTooHigh() {
        int customMaxRules = randomIntBetween(1001, Integer.MAX_VALUE);
        Settings settings = createCustomSettings("max_rules_per_ruleset", Integer.toString(customMaxRules));
        Exception e = expectThrows(IllegalArgumentException.class, () -> QueryRulesConfig.MAX_RULE_LIMIT_SETTING.get(settings));
        assertThat(e.getMessage(), containsString("[xpack.applications.rules.max_rules_per_ruleset] must be <= 1000"));
    }

    public void testCustomQueryRulesConfigMaxRulesPerRulesetTooLow() {
        int maxRules = randomIntBetween(Integer.MIN_VALUE, 0);
        Settings settings = createCustomSettings("max_rules_per_ruleset", Integer.toString(maxRules));
        Exception e = expectThrows(IllegalArgumentException.class, () -> QueryRulesConfig.MAX_RULE_LIMIT_SETTING.get(settings));
        assertThat(e.getMessage(), containsString("[xpack.applications.rules.max_rules_per_ruleset] must be >= 1"));
    }

    private static Settings createCustomSettings(String key, String value) {
        key = QueryRulesConfig.SETTING_ROOT_PATH + "." + key;
        return Settings.builder().put(key, value).build();
    }
}
