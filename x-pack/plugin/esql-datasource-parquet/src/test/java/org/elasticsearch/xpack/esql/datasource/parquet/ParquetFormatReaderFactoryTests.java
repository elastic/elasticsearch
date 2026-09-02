/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetFormatReaderFactoryTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testPluginFactoryReturnsCapabilityFactory() throws Exception {
        FormatReaderFactory factory = new ParquetDataSourcePlugin().formatReaders(Settings.EMPTY).get(FormatNameResolver.FORMAT_PARQUET);

        assertCapabilities(factory);
        assertEquals(FormatNameResolver.FORMAT_PARQUET, factory.formatName());
        assertNotNull(factory.aggregatePushdownSupport());
        assertNotNull(factory.filterPushdownSupport());
        assertFalse(factory.dropsRowsUnderPushedFilter());
        assertFalse(factory.supportsWholeFileCompression());
        assertFalse(factory.supportsBatchRead());
        try (ParquetFormatReader reader = (ParquetFormatReader) factory.create(Settings.EMPTY, BLOCK_FACTORY)) {
            assertNotNull(reader.statusSnapshot());
        }
    }

    public void testCreateReturnsDistinctReadersSharingCodecFactory() throws Exception {
        ParquetFormatReaderFactory factory = new ParquetFormatReaderFactory();
        ParquetFormatReader first = (ParquetFormatReader) factory.create(Settings.EMPTY, BLOCK_FACTORY);
        ParquetFormatReader second = (ParquetFormatReader) factory.create(Settings.EMPTY, BLOCK_FACTORY);
        try (first; second) {
            assertNotSame(first, second);
            assertSame(first.codecFactory(), second.codecFactory());
            assertNotSame(first.counters(), second.counters());
            first.counters().addRowsEmitted(3);
            assertEquals(3L, first.statusSnapshot().rowsEmitted());
            assertEquals(0L, second.statusSnapshot().rowsEmitted());
        }
    }

    public void testCreateAppliesBinding() throws Exception {
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(true, false).get();
        DynamicThreshold threshold = new DynamicThreshold("id", ElementType.LONG, true, false, channel);
        ParquetPushedExpressions pushedExpressions = new ParquetPushedExpressions(List.of());
        ParquetFormatReader configured = null;
        ParquetFormatReader unrecognized = null;
        try {
            configured = (ParquetFormatReader) new ParquetFormatReaderFactory().create(
                Settings.EMPTY,
                BLOCK_FACTORY,
                null,
                FormatReadContext.Binding.empty()
                    .withPushedFilter(pushedExpressions)
                    .withDynamicThreshold(threshold)
                    .withDeclaredDateFormats(Map.of("id", "epoch_millis"))
                    .withDeclaredTypeColumns(Set.of("id"))
            );
            assertSame(threshold, configured.dynamicThreshold());
            assertSame(pushedExpressions, configured.pushedExpressions());
            assertSame(FilterCompat.NOOP, configured.pushedFilter());
            assertFalse(configured.forceBaselinePath());
            assertTrue(configured.optimizedReader());
            assertNotNull(configured.declaredDateFormatterFor("id"));
            assertTrue(configured.isDeclaredTypeColumn("id"));
            assertNotNull(configured.statusSnapshot());

            unrecognized = (ParquetFormatReader) new ParquetFormatReaderFactory().create(
                Settings.EMPTY,
                BLOCK_FACTORY,
                null,
                FormatReadContext.Binding.empty().withPushedFilter("not a filter")
            );
            assertNull(unrecognized.pushedExpressions());
            assertSame(FilterCompat.NOOP, unrecognized.pushedFilter());
        } finally {
            if (configured != null) {
                configured.close();
            }
            if (unrecognized != null) {
                unrecognized.close();
            }
            boolean retained = channel.tryIncRef();
            if (retained) {
                channel.decRef();
                threshold.close();
            }
            assertTrue("closing built readers must not close the borrowed threshold", retained);
        }
    }

    public void testInspectRecognizesNoKeys() {
        ParquetFormatReaderFactory factory = new ParquetFormatReaderFactory();
        assertTrue(factory.inspect(Map.of()).consumedKeys().isEmpty());
        assertTrue(factory.inspect(null).consumedKeys().isEmpty());
        assertTrue(factory.inspect(Map.of("not_a_parquet_key", true)).consumedKeys().isEmpty());
    }

    private static void assertCapabilities(FormatReaderFactory factory) {
        assertTrue(factory.rangeAware());
        assertTrue(factory.columnExtractor());
        assertTrue(factory.acceptsDynamicThreshold());
        assertFalse(factory.supportsWholeFileCompression());
        assertEquals(ErrorPolicy.STRICT, factory.defaultErrorPolicy());
    }
}
