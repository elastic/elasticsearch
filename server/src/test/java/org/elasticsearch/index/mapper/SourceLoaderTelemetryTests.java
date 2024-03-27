/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SourceLoaderTelemetryTests extends MapperServiceTestCase {
    private final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(telemetryPlugin);
    }

    @Override
    public void testFieldHasValue() {}

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {}

    public void testSyntheticSourceTelemetry() throws IOException {
        var mapping = syntheticSourceMapping(b -> { b.startObject("kwd").field("type", "keyword").endObject(); });

        var mapperService = createMapperService(mapping);

        assertThat(syntheticSource(mapperService, b -> b.field("kwd", "foo")), equalTo("""
            {"kwd":"foo"}"""));

        var measurements = telemetryPlugin.getLongHistogramMeasurement(SourceFieldMetrics.SYNTHETIC_SOURCE_LOAD_LATENCY);
        assertEquals(1, measurements.size());
        // test implementation of time provider always has a gap of 1 between values
        assertEquals(measurements.get(0).getLong(), 1);
    }
}
