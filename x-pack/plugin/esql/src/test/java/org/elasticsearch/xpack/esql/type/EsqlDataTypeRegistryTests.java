/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EsqlDataTypeRegistryTests extends ESTestCase {

    public void testCounter() {
        resolve("long", TimeSeriesParams.MetricType.COUNTER, DataType.COUNTER_LONG);
        resolve("integer", TimeSeriesParams.MetricType.COUNTER, DataType.COUNTER_INTEGER);
        resolve("double", TimeSeriesParams.MetricType.COUNTER, DataType.COUNTER_DOUBLE);
    }

    public void testGauge() {
        resolve("long", TimeSeriesParams.MetricType.GAUGE, DataType.LONG);
    }

    public void testLong() {
        resolve("long", null, DataType.LONG);
    }

    /**
     * When the {@code esql.query.flattened.enabled} kill switch is off ({@code FieldsInfo.flattenedDataTypeEnabled() == false}),
     * a {@code flattened} field must resolve as {@link DataType#UNSUPPORTED} instead of {@link DataType#FLATTENED}. Asserting
     * both states proves it is the kill switch, not the transport version, that drives the {@code unsupported} result.
     */
    public void testFlattenedResolvesAsUnsupportedWhenKillSwitchDisabled() {
        assertThat(resolveType("flattened", true), equalTo(DataType.FLATTENED));
        assertThat(resolveType("flattened", false), equalTo(DataType.UNSUPPORTED));
    }

    /**
     * The flattened kill switch must only affect {@code flattened}: other types resolve normally even when the flag is off.
     */
    public void testFlattenedKillSwitchDoesNotAffectOtherTypes() {
        assertThat(resolveType("keyword", false), equalTo(DataType.KEYWORD));
        assertThat(resolveType("long", false), equalTo(DataType.LONG));
    }

    private DataType resolveType(String esTypeName, boolean flattenedDataTypeEnabled) {
        String idx = "idx-" + randomAlphaOfLength(5);
        String field = "f" + randomAlphaOfLength(3);
        List<FieldCapabilitiesIndexResponse> idxResponses = List.of(
            new FieldCapabilitiesIndexResponse(
                idx,
                idx,
                Map.of(field, new IndexFieldCapabilitiesBuilder(field, esTypeName).build()),
                true,
                IndexMode.STANDARD
            )
        );
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(idxResponses).build();
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, flattenedDataTypeEnabled),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        return resolution.get().mapping().get(field).getDataType();
    }

    private void resolve(String esTypeName, TimeSeriesParams.MetricType metricType, DataType expected) {
        String idx = "idx-" + randomAlphaOfLength(5);
        String field = "f" + randomAlphaOfLength(3);
        List<FieldCapabilitiesIndexResponse> idxResponses = List.of(
            new FieldCapabilitiesIndexResponse(
                idx,
                idx,
                Map.of(field, new IndexFieldCapabilitiesBuilder(field, esTypeName).metricType(metricType).build()),
                true,
                IndexMode.TIME_SERIES
            )
        );

        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(idxResponses).build();
        // IndexResolver uses EsqlDataTypeRegistry directly
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, true),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        EsField f = resolution.get().mapping().get(field);
        assertThat(f.getDataType(), equalTo(expected));
    }
}
