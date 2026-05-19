/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;

import java.util.Map;

/**
 * Verifies that {@link PushFiltersToSource#resolveFormatName} delegates to
 * {@link FormatNameResolver#resolve}. Comprehensive resolution tests live in
 * {@link org.elasticsearch.xpack.esql.datasources.FormatNameResolverTests}.
 */
public class PushFiltersToSourceTests extends ESTestCase {

    public void testResolveFormatNameDelegatesToFormatNameResolver() {
        assertEquals(
            FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"),
            PushFiltersToSource.resolveFormatName(Map.of("reader", "java"), "file.parquet")
        );
    }

    public void testResolveFormatNameFromExtension() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(null, "s3://bucket/data/file.orc"));
    }
}
