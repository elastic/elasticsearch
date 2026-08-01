/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;

public class DerivedMetricsDestinationTests extends ESTestCase {

    public void testDestinationNaming() {
        assertEquals("derived-metrics-logs-my_app-default-10s", DerivedMetricsDestination.destinationFor("logs-my_app-default", "10s"));
        assertEquals("derived-metrics-logs-my_app-default-1m", DerivedMetricsDestination.destinationFor("logs-my_app-default", "1m"));
        assertTrue(DerivedMetricsDestination.isDestination("derived-metrics-logs-my_app-default-10s"));
        assertFalse(DerivedMetricsDestination.isDestination("logs-my_app-default"));
    }

    /**
     * The destination must be a hidden time series data stream that can be auto created, since the first metric document written to it
     * is what brings it into existence.
     */
    public void testTemplateIsAHiddenAutoCreatedTimeSeriesDataStream() {
        ComposableIndexTemplate template = DerivedMetricsDestination.template();
        assertThat(template.indexPatterns(), contains("derived-metrics-*"));
        assertNotNull(template.getDataStreamTemplate());
        assertTrue(template.getDataStreamTemplate().isHidden());
        assertEquals(Boolean.TRUE, template.getAllowAutoCreate());
        assertEquals(DerivedMetricsDestination.TEMPLATE_VERSION, template.version().longValue());

        var settings = template.template().settings();
        assertEquals("time_series", settings.get("index.mode"));
        assertEquals(List.of("metric.name", "derived_metrics.*", "dimensions.*"), settings.getAsList("index.routing_path"));
    }

    @SuppressWarnings("unchecked")
    public void testTemplateMapsInternalDimensionsAndTheMetricValue() {
        Map<String, Object> mapping = XContentHelper.convertToMap(
            DerivedMetricsDestination.template().template().mappings().uncompressed(),
            true,
            XContentType.JSON
        ).v2();
        Map<String, Object> properties = (Map<String, Object>) ((Map<String, Object>) mapping.get("_doc")).get("properties");

        Map<String, Object> derivedMetrics = (Map<String, Object>) ((Map<String, Object>) properties.get("derived_metrics")).get(
            "properties"
        );
        for (String dimension : List.of("source", "interval", "node")) {
            Map<String, Object> field = (Map<String, Object>) derivedMetrics.get(dimension);
            assertEquals("keyword", field.get("type"));
            assertEquals(Boolean.TRUE, field.get("time_series_dimension"));
        }

        Map<String, Object> metric = (Map<String, Object>) ((Map<String, Object>) properties.get("metric")).get("properties");
        assertEquals(Boolean.TRUE, ((Map<String, Object>) metric.get("name")).get("time_series_dimension"));
        Map<String, Object> value = (Map<String, Object>) metric.get("value");
        assertEquals("double", value.get("type"));
        assertEquals("gauge", value.get("time_series_metric"));

        assertThat(properties, hasKey("dimensions"));
    }

    /**
     * A destination is named after its source, so a source long enough pushes the backing index over the index name limit. Left
     * unchecked the configuration is accepted and no metric is ever emitted, so the boundary is worth pinning down exactly.
     */
    public void testADestinationThatWouldBeUnnameableIsRejected() {
        // ".ds-" + "-uuuu.MM.dd" + "-000001" is 22 bytes on top of the destination name, and the destination adds
        // "derived-metrics-" and "-10s" on top of the source: 255 - 22 - 16 - 4 leaves 213 bytes of source name
        String longestAllowed = "l".repeat(213);
        DerivedMetricsDestination.validateDestinationName(longestAllowed, "10s");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DerivedMetricsDestination.validateDestinationName(longestAllowed + "x", "10s")
        );
        assertThat(e.getMessage(), containsString("over the limit of 255"));
        assertThat(e.getMessage(), containsString("shorten the data stream name by at least 1 bytes"));
    }

    /**
     * The limit is on bytes rather than characters, so a name of allowable length in characters can still be too long.
     */
    public void testTheLimitIsMeasuredInBytesNotCharacters() {
        // each of these is three bytes in UTF-8, so 71 of them is 213 bytes: the longest allowed
        DerivedMetricsDestination.validateDestinationName("\u00e9\u00e9\u00e9".repeat(0) + "\u4e2d".repeat(71), "10s");
        expectThrows(IllegalArgumentException.class, () -> DerivedMetricsDestination.validateDestinationName("\u4e2d".repeat(72), "10s"));
    }

    /**
     * A longer interval leaves less room for the name, so the check has to be made per interval rather than once per stream.
     */
    public void testALongerIntervalLeavesLessRoom() {
        String source = "l".repeat(212);
        DerivedMetricsDestination.validateDestinationName(source, "10s");
        expectThrows(IllegalArgumentException.class, () -> DerivedMetricsDestination.validateDestinationName(source, "1440m"));
    }
}
