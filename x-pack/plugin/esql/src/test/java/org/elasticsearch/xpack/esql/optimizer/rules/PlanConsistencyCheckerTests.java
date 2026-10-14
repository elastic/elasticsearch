/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Set;

/** Metadata source projections share a name, but ordinary output names and all column ids must remain unique. */
public class PlanConsistencyCheckerTests extends ESTestCase {
    public void testDistinctMetadataProjections() {
        var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var residual = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
        assertFalse(check(List.of(full, residual)).hasFailures());
        assertTrue(check(List.of(full, residual.withId(full.id()))).hasFailures());
    }

    public void testMetadataDoesNotHideDuplicateOutputNames() {
        var metadata = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var alias = new Alias(Source.EMPTY, metadata.name(), metadata).toAttribute();
        assertTrue(check(List.of(metadata, alias)).hasFailures());
        assertTrue(check(List.of(alias, metadata)).hasFailures());
        assertTrue(check(List.of(alias, alias.withId(metadata.id()))).hasFailures());
    }

    private static Failures check(List<Attribute> attributes) {
        var failures = new Failures();
        PlanConsistencyChecker.checkPlan(EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(attributes), failures);
        return failures;
    }
}
