/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FieldSemanticsTests extends ESTestCase {

    private static IndexFieldCapabilities caps(
        String name,
        String type,
        boolean dimension,
        TimeSeriesParams.MetricType metricType,
        Map<String, String> meta
    ) {
        return caps(name, type, false, true, true, false, dimension, metricType, meta);
    }

    private static IndexFieldCapabilities caps(
        String name,
        String type,
        boolean metadata,
        boolean searchable,
        boolean aggregatable,
        boolean inference,
        boolean dimension,
        TimeSeriesParams.MetricType metricType,
        Map<String, String> meta
    ) {
        return new IndexFieldCapabilities(
            name,
            type,
            metadata,
            searchable,
            aggregatable,
            inference,
            dimension,
            metricType,
            null,
            meta,
            null,
            0,
            false
        );
    }

    private static FieldSemantics of(IndexFieldCapabilities... caps) {
        return FieldSemantics.from(caps[0].name(), List.of(caps));
    }

    public void testDeclaredTimeSeriesRoles() {
        FieldSemantics dimension = of(caps("host", "keyword", true, null, Map.of()));
        assertThat(dimension.role(), equalTo(FieldSemantics.Role.DIMENSION));
        assertThat(dimension.metricType(), nullValue());

        FieldSemantics counter = of(caps("bytes", "long", false, TimeSeriesParams.MetricType.COUNTER, Map.of("unit", "By")));
        assertThat(counter.role(), equalTo(FieldSemantics.Role.METRIC));
        assertThat(counter.metricType(), equalTo("counter"));
        assertThat(counter.unit(), equalTo("By"));
    }

    public void testRolesInferredFromTypes() {
        assertThat(of(caps("@timestamp", "date", false, null, Map.of())).role(), equalTo(FieldSemantics.Role.TIME));
        assertThat(
            "only @timestamp is the time",
            of(caps("created", "date", false, null, Map.of())).role(),
            equalTo(FieldSemantics.Role.ATTRIBUTE)
        );
        assertThat(of(caps("client.ip", "ip", false, null, Map.of())).role(), equalTo(FieldSemantics.Role.ATTRIBUTE));
        assertThat(of(caps("bytes", "long", false, null, Map.of())).role(), equalTo(FieldSemantics.Role.NUMERIC));
        assertThat(of(caps("message", "text", false, null, Map.of())).role(), equalTo(FieldSemantics.Role.TEXT));
        assertThat(of(caps("embedding", "dense_vector", false, null, Map.of())).role(), equalTo(FieldSemantics.Role.VECTOR));
        assertThat(of(caps("shape", "shape", false, null, Map.of())).role(), equalTo(FieldSemantics.Role.OTHER));
        assertThat(
            of(caps("_index", "_index", true, true, true, false, false, null, Map.of())).role(),
            equalTo(FieldSemantics.Role.METADATA)
        );
        assertThat(
            of(caps("body", "semantic_text", false, true, false, true, false, null, Map.of())).role(),
            equalTo(FieldSemantics.Role.SEMANTIC)
        );
    }

    public void testDescriptionFromMeta() {
        FieldSemantics field = of(caps("latency", "double", false, null, Map.of("unit", "ms", "description", "Request latency")));
        assertThat(field.unit(), equalTo("ms"));
        assertThat(field.description(), equalTo("Request latency"));
    }

    public void testMergesAcrossIndices() {
        FieldSemantics agree = of(
            caps("latency", "double", false, TimeSeriesParams.MetricType.GAUGE, Map.of("unit", "ms")),
            caps("latency", "double", false, TimeSeriesParams.MetricType.GAUGE, Map.of("unit", "ms"))
        );
        assertThat(agree.role(), equalTo(FieldSemantics.Role.METRIC));
        assertThat(agree.unit(), equalTo("ms"));

        FieldSemantics units = of(
            caps("latency", "double", false, TimeSeriesParams.MetricType.GAUGE, Map.of("unit", "ms")),
            caps("latency", "double", false, TimeSeriesParams.MetricType.GAUGE, Map.of("unit", "s"))
        );
        assertThat("disagreeing units are all reported", units.unit(), equalTo("ms,s"));

        FieldSemantics roles = of(
            caps("value", "long", false, TimeSeriesParams.MetricType.COUNTER, Map.of()),
            caps("value", "long", false, null, Map.of())
        );
        assertThat(roles.role(), equalTo(FieldSemantics.Role.MIXED));

        FieldSemantics types = of(
            caps("value", "long", false, TimeSeriesParams.MetricType.COUNTER, Map.of()),
            caps("value", "long", false, TimeSeriesParams.MetricType.GAUGE, Map.of())
        );
        assertThat(types.metricType(), equalTo("mixed"));
    }

    public void testOnlyBuiltWhenNeeded() {
        IndexFieldCapabilities bytes = caps("bytes", "long", false, TimeSeriesParams.MetricType.COUNTER, Map.of("unit", "By"));
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.builder()
            .withIndexResponses(
                List.of(new FieldCapabilitiesIndexResponse("idx", "idx", Map.of("bytes", bytes), true, IndexMode.TIME_SERIES, 1, 0, 1L, 1L))
            )
            .build();
        assertThat(resolve(response, true).get("bytes").metricType(), equalTo("counter"));
        assertThat(resolve(response, false), anEmptyMap());
    }

    private static Map<String, FieldSemantics> resolve(FieldCapabilitiesResponse response, boolean needsFieldSemantics) {
        IndexResolver.FieldsInfo info = new IndexResolver.FieldsInfo(
            response,
            TransportVersion.current(),
            false,
            false,
            false,
            false,
            false,
            needsFieldSemantics,
            false,
            null
        );
        return IndexResolver.mergedMappings("idx", false, info, false, IndexResolver.DO_NOT_GROUP).get().fieldSemantics();
    }
}
