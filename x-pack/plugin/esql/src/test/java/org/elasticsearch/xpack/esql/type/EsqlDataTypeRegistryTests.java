/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EsqlDataTypeRegistryTests extends ESTestCase {
    public void testCounter() {
        resolve("long", TimeSeriesParams.MetricType.COUNTER, DataTypes.UNSIGNED_LONG);
    }

    public void testGauge() {
        resolve("long", TimeSeriesParams.MetricType.GAUGE, DataTypes.LONG);
    }

    public void testLong() {
        resolve("long", null, DataTypes.LONG);
    }

    private void resolve(String esTypeName, TimeSeriesParams.MetricType metricType, DataType expected) {
        String[] indices = new String[] { "idx-" + randomAlphaOfLength(5) };
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            indices,
            Map.of(
                "f",
                Map.of(
                    esTypeName,
                    new FieldCapabilities(
                        "c",
                        esTypeName,
                        false,
                        true,
                        true,
                        false,
                        metricType,
                        indices,
                        Strings.EMPTY_ARRAY,
                        Strings.EMPTY_ARRAY,
                        Strings.EMPTY_ARRAY,
                        Strings.EMPTY_ARRAY,
                        Map.of()
                    )
                )
            )
        );
        IndexResolution resolution = IndexResolver.mergedMappings(EsqlDataTypeRegistry.INSTANCE, "idx-*", caps);

        EsField f = resolution.get().mapping().get("f");
        assertThat(f.getDataType(), equalTo(expected));
    }
}
