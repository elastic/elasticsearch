/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class UnmappedFieldsPatternTests extends ESTestCase {

    public void testSingleIncludeGroupUsesOrSemantics() {
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.includes(List.of("first_name*", "salary_bonus*"));
        assertTrue(pattern.matches("first_name_suffix"));
        assertTrue(pattern.matches("salary_bonus"));
        assertFalse(pattern.matches("first_grade"));
    }

    public void testIntersectCombinesOrGroupsWithAnd() {
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.includes(List.of("first*", "salary_bonus*"))
            .intersect(UnmappedFieldsPattern.includes(List.of("first_name*")));
        assertTrue(pattern.matches("first_name_suffix"));
        assertFalse(pattern.matches("salary_bonus"));
        assertFalse(pattern.matches("first_grade"));
    }

    public void testExcludesApplyAfterIncludes() {
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.includes(List.of("first*")).withAdditionalExcludes(List.of("first_name"));
        assertTrue(pattern.matches("first_pet"));
        assertFalse(pattern.matches("first_name"));
    }

    public void testAllMatchesAnyNameUnlessExcluded() {
        assertTrue(UnmappedFieldsPattern.ALL.matches("anything"));
        assertTrue(UnmappedFieldsPattern.excludes(List.of("secret*")).matches("public"));
        assertFalse(UnmappedFieldsPattern.excludes(List.of("secret*")).matches("secret_key"));
    }

    public void testNoneMatchesNothing() {
        assertFalse(UnmappedFieldsPattern.NONE.matches("anything"));
    }
}
