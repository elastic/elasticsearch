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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIP;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.session.EsqlIndexResolver;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIP;
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

    public void testMultiTypesToIp() {
        var source = new Source(0, 0, "");
        Map<String, Expression> typeConversions = new HashMap<>();
        typeConversions.put("ip", new ToIP(source, new Literal(source, stringToIP("10.10.0.1"), DataTypes.IP)));
        typeConversions.put("keyword", new ToIP(source, new Literal(source, "127.0.0.1", DataTypes.KEYWORD)));
        resolveMulti(typeConversions, DataTypes.IP);
    }

    public void testMultiTypesToString() {
        var source = new Source(0, 0, "");
        Map<String, Expression> typeConversions = new HashMap<>();
        typeConversions.put("ip", new ToString(source, new Literal(source, stringToIP("10.10.0.1"), DataTypes.IP)));
        typeConversions.put("keyword", new ToString(source, new Literal(source, "127.0.0.1", DataTypes.KEYWORD)));
        resolveMulti(typeConversions, DataTypes.KEYWORD);
    }

    private void resolveMulti(Map<String, Expression> typeConversions, DataType expectedResolvedType) {
        String field = "f" + randomAlphaOfLength(3);
        List<FieldCapabilitiesIndexResponse> idxResponses = new ArrayList<>();
        for (String typeName : typeConversions.keySet()) {
            String idx = "idx-" + randomAlphaOfLength(5);
            idxResponses.add(
                new FieldCapabilitiesIndexResponse(
                    idx,
                    idx,
                    Map.of(field, new IndexFieldCapabilities(field, typeName, false, true, true, false, null, Map.of())),
                    true
                )
            );
        }
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(idxResponses, List.of());
        IndexResolution resolution = new EsqlIndexResolver(null, EsqlDataTypeRegistry.INSTANCE).mergedMappings("idx-*", caps);
        EsField f = resolution.get().mapping().get(field);
        assertThat("Expecting a multi-type field", f, instanceOf(MultiTypeEsField.UnresolvedField.class));
        MultiTypeEsField.UnresolvedField mtf = (MultiTypeEsField.UnresolvedField) f;
        Map<String, Set<String>> typesToIndices = mtf.getTypesToIndices();
        Set<String> expected = typeConversions.keySet();
        assertThat(typesToIndices.keySet(), equalTo(expected));

        // Before type resolution we expect the resolved type to be unsupported
        assertThat(f.getDataType(), equalTo(DataTypes.UNSUPPORTED));

        // After type resolution we expect the resolved type to be the expected type
        MultiTypeEsField resolved = mtf.resolve(typeConversions);
        assertThat(resolved.getDataType(), equalTo(expectedResolvedType));
    }
}
