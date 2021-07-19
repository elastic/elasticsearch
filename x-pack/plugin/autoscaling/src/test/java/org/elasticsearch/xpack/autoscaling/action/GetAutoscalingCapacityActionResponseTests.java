/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResults;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GetAutoscalingCapacityActionResponseTests extends AutoscalingTestCase {

    public void testToXContent() throws IOException {
        Set<String> policyNames = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> randomAlphaOfLength(10))
            .collect(Collectors.toSet());

        SortedMap<String, AutoscalingDeciderResults> results = new TreeMap<>(
            policyNames.stream().map(s -> Tuple.tuple(s, randomAutoscalingDeciderResults())).collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );

        GetAutoscalingCapacityAction.Response response = new GetAutoscalingCapacityAction.Response(results);
        XContentType xContentType = randomFrom(XContentType.values());

        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        response.toXContent(builder, null);
        BytesReference responseBytes = BytesReference.bytes(builder);

        XContentBuilder expected = XContentBuilder.builder(xContentType.xContent());
        expected.startObject();
        expected.startObject("policies");
        for (Map.Entry<String, AutoscalingDeciderResults> entry : results.entrySet()) {
            expected.field(entry.getKey(), entry.getValue());
        }
        expected.endObject();
        expected.endObject();
        BytesReference expectedBytes = BytesReference.bytes(expected);
        assertThat(responseBytes, Matchers.equalTo(expectedBytes));
    }
}
