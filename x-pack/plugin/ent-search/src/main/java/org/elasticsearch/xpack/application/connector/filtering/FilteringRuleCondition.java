/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

public enum FilteringRuleCondition {
    CONTAINS("contains"),
    ENDS_WITH("ends_with"),
    EQUALS("equals"),
    GT(">"),
    LT("<"),
    REGEX("regex"),
    STARTS_WITH("starts_with");

    private final String value;

    FilteringRuleCondition(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    public static FilteringRuleCondition filteringRuleCondition(String condition) {
        for (FilteringRuleCondition filteringRuleCondition : FilteringRuleCondition.values()) {
            if (filteringRuleCondition.value.equals(condition)) {
                return filteringRuleCondition;
            }
        }
        throw new IllegalArgumentException("Unknown " + FilteringRuleCondition.class.getSimpleName() + " [" + condition + "].");
    }
}
