/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SourceFieldMetricsTests extends MapperServiceTestCase {
    private final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(telemetryPlugin);
    }

    @Override
    public void testFieldHasValue() {}

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {}

    public void testSyntheticSourceLoadLatency() throws IOException {
        var mapping = syntheticSourceMapping(b -> b.startObject("kwd").field("type", "keyword").endObject());
        var mapper = createDocumentMapper(mapping);

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapper.parse(source(b -> b.field("kwd", "foo"))).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SourceProvider provider = SourceProvider.fromSyntheticSource(
                    mapper.mapping(),
                    createTestMapperMetrics().sourceFieldMetrics()
                );
                Source synthetic = provider.getSource(getOnlyLeafReader(reader).getContext(), 0);
                assertEquals(synthetic.source().get("kwd"), "foo");
            }
        }

        var measurements = telemetryPlugin.getLongHistogramMeasurement(SourceFieldMetrics.SYNTHETIC_SOURCE_LOAD_LATENCY);
        assertEquals(1, measurements.size());
        // test implementation of time provider always has a gap of 1 between values
        assertEquals(measurements.get(0).getLong(), 1);
    }

    public void testSyntheticSourceIncompatibleMapping() throws IOException {
        var mapping = syntheticSourceMapping(b -> b.startObject("kwd").field("type", "text").field("store", "false").endObject());
        var mapperMetrics = createTestMapperMetrics();
        var mapperService = new TestMapperServiceBuilder().mapperMetrics(mapperMetrics).build();
        assertThrows(IllegalArgumentException.class, () -> withMapping(mapperService, mapping));

        var measurements = telemetryPlugin.getLongCounterMeasurement(SourceFieldMetrics.SYNTHETIC_SOURCE_INCOMPATIBLE_MAPPING);
        assertEquals(1, measurements.size());
        assertEquals(measurements.get(0).getLong(), 1);
    }
}
