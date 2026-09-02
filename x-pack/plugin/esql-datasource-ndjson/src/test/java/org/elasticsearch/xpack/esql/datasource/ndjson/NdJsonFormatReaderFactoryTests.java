/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

public class NdJsonFormatReaderFactoryTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testPluginFactoryCreatesDistinctReaders() {
        FormatReaderFactory factory = new NdJsonDataSourcePlugin().formatReaders(Settings.EMPTY).get("ndjson");

        assertThat(factory, instanceOf(NdJsonFormatReaderFactory.class));
        assertTrue(factory.segmentable());

        FormatReader first = factory.create(Settings.EMPTY, blockFactory);
        FormatReader second = factory.create(Settings.EMPTY, blockFactory);

        assertThat(first, instanceOf(NdJsonFormatReader.class));
        assertThat(first, instanceOf(SegmentableFormatReader.class));
        assertNotSame(first, second);
    }

    public void testFactoryStateIsFinal() {
        for (Field field : NdJsonFormatReaderFactory.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) == false) {
                assertTrue("field [" + field.getName() + "] must be final", Modifier.isFinal(field.getModifiers()));
            }
        }
    }

    public void testInspectReportsRecognizedKeysAndCreateFingerprintsCompleteConfig() throws IOException {
        Map<String, Object> config = Map.of("datetime_format", "strict_date_optional_time", "error_mode", "skip_row");
        NdJsonFormatReaderFactory factory = new NdJsonFormatReaderFactory(Settings.EMPTY);
        Configured<Void> inspected = factory.inspect(config);

        assertThat(inspected.consumedKeys(), containsInAnyOrder("datetime_format"));
        NdJsonFormatReader reader = (NdJsonFormatReader) factory.create(
            Settings.EMPTY,
            blockFactory,
            config,
            FormatReadContext.Binding.empty()
        );
        var object = new BytesStorageObject("memory://factory-config.ndjson", "{\"a\":1}\n".getBytes(StandardCharsets.UTF_8));

        assertEquals(
            SchemaCacheKey.buildFormatConfig(config),
            reader.metadata(object).sourceMetadata().get(ExternalStats.CONFIG_FINGERPRINT_KEY)
        );
    }

    public void testCreateReturnsDistinctReadersWithIndependentCounters() throws IOException {
        NdJsonFormatReaderFactory factory = new NdJsonFormatReaderFactory(Settings.EMPTY);
        FormatReadContext.Binding binding = FormatReadContext.Binding.empty().withBoundSchema(SCHEMA);
        NdJsonFormatReader first = (NdJsonFormatReader) factory.create(Settings.EMPTY, blockFactory, null, binding);
        NdJsonFormatReader second = (NdJsonFormatReader) factory.create(Settings.EMPTY, blockFactory, null, binding);
        NdJsonFormatReader isolated = (NdJsonFormatReader) factory.create(Settings.EMPTY, blockFactory);

        assertNotSame(first, second);
        drain(first);

        assertTrue(first.statusSnapshot().readNanos() > 0);
        assertEquals(0L, second.statusSnapshot().readNanos());
        assertEquals(0L, isolated.statusSnapshot().readNanos());

        long firstCpu = first.statusSnapshot().readCpuNanos();
        second.acceptReadCpuNanos(99_999L);
        assertEquals(firstCpu, first.statusSnapshot().readCpuNanos());
        assertEquals(99_999L, second.statusSnapshot().readCpuNanos());
    }

    public void testFactoryExposesConfiguredCapabilities() {
        Settings settings = Settings.builder().put(NdJsonFormatReader.SEGMENT_SIZE_SETTING, "128kb").build();
        NdJsonFormatReaderFactory factory = new NdJsonFormatReaderFactory(settings);

        assertEquals("ndjson", factory.formatName());
        assertTrue(factory.segmentable());
        assertEquals(ErrorPolicy.STRICT, factory.defaultErrorPolicy());
        assertEquals(128L * 1024L, factory.minimumSegmentSize(null));
        assertThat(factory.recordSplitter(null, 1024), instanceOf(NdJsonRecordSplitter.class));
        assertThat(factory.aggregatePushdownSupport(), instanceOf(TextAggregatePushdownSupport.class));
    }

    private static final List<Attribute> SCHEMA = List.of(
        new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, null, "b", DataType.KEYWORD)
    );

    private void drain(NdJsonFormatReader reader) throws IOException {
        String ndjson = """
            {"a": 1, "b": "x"}
            {"a": 2, "b": "y"}
            """;
        var object = new BytesStorageObject("memory://factory-counters.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }
    }
}
