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
        QueryRulesConfig config = new QueryRulesConfig(Settings.EMPTY);

        assertThat(config.maxRulesPerRuleset(), equalTo(100));
    }

    public void testCustomQueryRulesConfigMaxRulesPerRuleset() {
        int maxRules = randomIntBetween(1, 1000);
        QueryRulesConfig config = createCustomConfig("max_rules_per_ruleset", Integer.toString(maxRules));
        assertThat(config.maxRulesPerRuleset(), equalTo(maxRules));
    }

    public void testCustomQueryRulesConfigMaxRulesPerRulesetTooHigh() {
        int maxRules = randomIntBetween(1001, Integer.MAX_VALUE);
        Exception e = 
            expectThrows(IllegalArgumentException.class, () -> createCustomConfig("max_rules_per_ruleset", Integer.toString(maxRules)));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.rules.max_rules_per_ruleset] must be <= 1000")
        );
    }

    public void testCustomQueryRulesConfigMaxRulesPerRulesetTooLow() {
        int maxRules = randomIntBetween(Integer.MIN_VALUE, 0);
        Exception e =
            expectThrows(IllegalArgumentException.class, () -> createCustomConfig("max_rules_per_ruleset", Integer.toString(maxRules)));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.rules.max_rules_per_ruleset] must be >= 1")
        );
    }

    private QueryRulesConfig createCustomConfig(String key, String value) {
        key = "xpack.applications.rules." + key;
        return new QueryRulesConfig(Settings.builder().put(key, value).build());
    }
}
