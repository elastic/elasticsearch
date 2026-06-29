/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import org.elasticsearch.test.ESTestCase;

public class ChildMemoryCircuitBreakerTests extends ESTestCase {

    public void testCategoryForNull() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor(null));
    }

    public void testCategoryForEmptyString() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor(""));
    }

    public void testCategoryForKnownCategories() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_QUERY, ChildMemoryCircuitBreaker.categoryFor("query"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_WILDCARD, ChildMemoryCircuitBreaker.categoryFor("wildcard"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_REGEXP, ChildMemoryCircuitBreaker.categoryFor("regexp"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_RANGE, ChildMemoryCircuitBreaker.categoryFor("range"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_PREALLOCATE, ChildMemoryCircuitBreaker.categoryFor("preallocate"));
    }

    public void testCategoryForCompositeLabelsBracket() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_PREALLOCATE, ChildMemoryCircuitBreaker.categoryFor("preallocate[aggregations]"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_PREALLOCATE, ChildMemoryCircuitBreaker.categoryFor("preallocate[test]"));
    }

    public void testCategoryForCompositeLabelsColon() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_RANGE, ChildMemoryCircuitBreaker.categoryFor("range:my_field"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_RANGE, ChildMemoryCircuitBreaker.categoryFor("range:field.keyword"));
    }

    public void testCategoryForUnknownLabel() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor("field_name_a"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor("indices:data/read/search"));
    }

    public void testCategoryForCaseSensitive() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor("WILDCARD"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor("Query"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor("RANGE:field"));
    }

    public void testCategoryForSeparatorAtStart() {
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor("[preallocate[test]"));
        assertEquals(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, ChildMemoryCircuitBreaker.categoryFor(":range:field"));
    }
}
