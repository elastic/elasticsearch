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
import static org.hamcrest.Matchers.hasKey;

public class DerivedMetricsDestinationTests extends ESTestCase {

    public void testDestinationNaming() {
        assertEquals("derived-metrics-logs-my_app-default", DerivedMetricsDestination.destinationFor("logs-my_app-default"));
        assertTrue(DerivedMetricsDestination.isDestination("derived-metrics-logs-my_app-default"));
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
}
