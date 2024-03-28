/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.session.EsqlIndexResolver;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class EsqlDataTypeRegistryTests extends ESTestCase {
    public void testCounter() {
        resolve("long", TimeSeriesParams.MetricType.COUNTER, DataTypes.UNSUPPORTED);
    }

    public void testGauge() {
        resolve("long", TimeSeriesParams.MetricType.GAUGE, DataTypes.LONG);
    }

    public void testLong() {
        resolve("long", null, DataTypes.LONG);
    }

    private void resolve(String esTypeName, TimeSeriesParams.MetricType metricType, DataType expected) {
        String idx = "idx-" + randomAlphaOfLength(5);
        String field = "f" + randomAlphaOfLength(3);
        List<FieldCapabilitiesIndexResponse> idxResponses = List.of(
            new FieldCapabilitiesIndexResponse(
                idx,
                idx,
                Map.of(field, new IndexFieldCapabilities(field, esTypeName, false, true, true, false, metricType, Map.of())),
                true
            )
        );

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(idxResponses, List.of());
        IndexResolution resolution = new EsqlIndexResolver(null, EsqlDataTypeRegistry.INSTANCE).mergedMappings("idx-*", caps);
        EsField f = resolution.get().mapping().get(field);
        assertThat(f.getDataType(), equalTo(expected));
    }

    public void testMulti() {
        resolveMulti(List.of(DataTypes.IP, DataTypes.KEYWORD));
    }

    private void resolveMulti(List<DataType> expectedTypes) {
        String field = "f" + randomAlphaOfLength(3);
        List<FieldCapabilitiesIndexResponse> idxResponses = new ArrayList<>();
        for (DataType t : expectedTypes) {
            String idx = "idx-" + randomAlphaOfLength(5);
            idxResponses.add(
                new FieldCapabilitiesIndexResponse(
                    idx,
                    idx,
                    Map.of(field, new IndexFieldCapabilities(field, t.esType(), false, true, true, false, null, Map.of())),
                    true
                )
            );
        }
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(idxResponses, List.of());
        IndexResolution resolution = new EsqlIndexResolver(null, EsqlDataTypeRegistry.INSTANCE).mergedMappings("idx-*", caps);
        EsField f = resolution.get().mapping().get(field);
        assertThat("Expecting a multi-type field", f, instanceOf(EsqlIndexResolver.MultiTypeField.class));
        EsqlIndexResolver.MultiTypeField mtf = (EsqlIndexResolver.MultiTypeField) f;
        Map<String, Set<String>> typesToIndices = mtf.getTypesToIndices();
        Set<String> expected = expectedTypes.stream().map(DataType::esType).collect(toSet());
        assertThat(typesToIndices.keySet(), equalTo(expected));
        // TODO: Decide what to do with multi-type fields
        assertThat(f.getDataType(), equalTo(DataTypes.UNSUPPORTED));
    }
}
