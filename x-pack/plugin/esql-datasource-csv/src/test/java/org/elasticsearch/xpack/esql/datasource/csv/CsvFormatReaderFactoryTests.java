/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

public class CsvFormatReaderFactoryTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testPluginFactoriesExposeCsvAndTsvCapabilities() {
        CsvDataSourcePlugin plugin = new CsvDataSourcePlugin();
        FormatReaderFactory csv = plugin.formatReaders(Settings.EMPTY).get("csv");
        FormatReaderFactory tsv = plugin.formatReaders(Settings.EMPTY).get("tsv");

        assertThat(csv, instanceOf(CsvFormatReaderFactory.class));
        assertThat(tsv, instanceOf(CsvFormatReaderFactory.class));
        assertTrue(csv.segmentable());
        assertTrue(tsv.segmentable());
        assertEquals("csv", csv.formatName());
        assertEquals("tsv", tsv.formatName());
        assertTrue(csv.headerRow(null));
        assertTrue(tsv.headerRow(null));
        assertFalse(csv.headerRow(Map.of("header_row", false)));
        assertThat(csv.recordSplitter(Map.of("mode", "plain"), 1024), instanceOf(NewlineRecordSplitter.class));
        assertEquals(1024L * 1024L, csv.minimumSegmentSize(null));
        assertThat(csv.inspect(Map.of("mode", "plain", "header_row", true)).consumedKeys(), containsInAnyOrder("mode", "header_row"));
        assertThat(csv.aggregatePushdownSupport(), instanceOf(TextAggregatePushdownSupport.class));
        assertTrue(plugin.formatSpecs().stream().anyMatch(spec -> spec.format().equals("csv") && spec.extensions().contains(".csv")));
        assertTrue(plugin.formatSpecs().stream().anyMatch(spec -> spec.format().equals("tsv") && spec.extensions().contains(".tsv")));
    }

    public void testCreateAppliesConfigAndBinding() {
        CsvFormatReaderFactory factory = new CsvFormatReaderFactory("csv", List.of(".csv"), CsvFormatOptions.DEFAULT, true);
        CsvFormatReader reader = (CsvFormatReader) factory.create(
            Settings.EMPTY,
            BLOCK_FACTORY,
            Map.of("mode", "plain", "header_row", true),
            FormatReadContext.Binding.empty()
                .withBoundSchema(List.of(new ReferenceAttribute(Source.EMPTY, null, "value", DataType.KEYWORD)))
                .withDeclaredDateFormats(Map.of("value", "yyyy-MM-dd"))
                .withDeclaredProvenanceBinding(true)
        );

        assertEquals("csv", factory.formatName());
        assertNotNull(reader);
        assertThat(factory.recordSplitter(Map.of("mode", "plain"), 1024), instanceOf(NewlineRecordSplitter.class));
    }

    public void testEachCreateHasIndependentCounters() {
        CsvFormatReaderFactory factory = new CsvFormatReaderFactory("csv", List.of(".csv"), CsvFormatOptions.DEFAULT, true);
        CsvFormatReader first = (CsvFormatReader) factory.create(
            Settings.EMPTY,
            BLOCK_FACTORY,
            Map.of("delimiter", "|"),
            FormatReadContext.Binding.empty()
        );
        CsvFormatReader second = (CsvFormatReader) factory.create(
            Settings.EMPTY,
            BLOCK_FACTORY,
            Map.of("delimiter", "|"),
            FormatReadContext.Binding.empty()
        );

        assertNotSame(first, second);
        first.acceptReadCpuNanos(123L);
        assertEquals(123L, first.statusSnapshot().readCpuNanos());
        assertEquals(0L, second.statusSnapshot().readCpuNanos());
        second.acceptReadCpuNanos(456L);
        assertEquals(123L, first.statusSnapshot().readCpuNanos());
        assertEquals(456L, second.statusSnapshot().readCpuNanos());
    }
}
