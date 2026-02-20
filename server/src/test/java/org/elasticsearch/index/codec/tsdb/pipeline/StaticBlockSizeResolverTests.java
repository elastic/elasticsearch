/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;

public class StaticBlockSizeResolverTests extends ESTestCase {

    private final BlockSizeResolver resolver = StaticBlockSizeResolver.INSTANCE;

    public void testTsdbReturns512() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "field",
            IndexMode.TIME_SERIES,
            null,
            null,
            PipelineConfig.DataType.LONG
        );
        assertEquals(StaticBlockSizeResolver.TSDB_BLOCK_SIZE, resolver.resolve(ctx));
    }

    public void testLogsdbReturns128() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "field",
            IndexMode.LOGSDB,
            null,
            null,
            PipelineConfig.DataType.LONG
        );
        assertEquals(StaticBlockSizeResolver.LOGSDB_BLOCK_SIZE, resolver.resolve(ctx));
    }

    public void testStandardReturnsDefault() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "field",
            IndexMode.STANDARD,
            null,
            null,
            PipelineConfig.DataType.LONG
        );
        assertEquals(StaticBlockSizeResolver.DEFAULT_BLOCK_SIZE, resolver.resolve(ctx));
    }

    public void testNullIndexModeReturnsDefault() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "field",
            null,
            null,
            null,
            PipelineConfig.DataType.LONG
        );
        assertEquals(StaticBlockSizeResolver.DEFAULT_BLOCK_SIZE, resolver.resolve(ctx));
    }
}
