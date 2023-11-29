/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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
        String[] indices = new String[] { "idx-" + randomAlphaOfLength(5) };
        FieldCapabilities fieldCap = new FieldCapabilities(
            randomAlphaOfLength(3),
            esTypeName,
            false,
            true,
            true,
            false,
            metricType,
            indices,
            null,
            null,
            null,
            null,
            Map.of()
        );
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(indices, Map.of(fieldCap.getName(), Map.of(esTypeName, fieldCap)));
        IndexResolution resolution = IndexResolver.mergedMappings(
            EsqlDataTypeRegistry.INSTANCE,
            "idx-*",
            caps,
            EsqlSession::specificValidity,
            IndexResolver.PRESERVE_PROPERTIES,
            null
        );

        EsField f = resolution.get().mapping().get(fieldCap.getName());
        assertThat(f.getDataType(), equalTo(expected));
    }
}
