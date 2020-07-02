/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisions;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GetAutoscalingDecisionActionResponseTests extends AutoscalingTestCase {

    public void testToXContent() throws IOException {
        Set<String> policyNames = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> randomAlphaOfLength(10))
            .collect(Collectors.toSet());

        SortedMap<String, AutoscalingDecisions> decisions = new TreeMap<>(
            policyNames.stream().map(s -> Tuple.tuple(s, randomAutoscalingDecisions())).collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );

        GetAutoscalingDecisionAction.Response response = new GetAutoscalingDecisionAction.Response(decisions);
        XContentType xContentType = randomFrom(XContentType.values());

        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        response.toXContent(builder, null);
        BytesReference responseBytes = BytesReference.bytes(builder);

        XContentBuilder expected = XContentBuilder.builder(xContentType.xContent());
        expected.startObject();
        expected.startArray("decisions");
        for (Map.Entry<String, AutoscalingDecisions> entry : decisions.entrySet()) {
            expected.startObject();
            expected.field(entry.getKey(), entry.getValue());
            expected.endObject();
        }
        expected.endArray();
        expected.endObject();
        BytesReference expectedBytes = BytesReference.bytes(expected);
        assertThat(responseBytes, Matchers.equalTo(expectedBytes));
    }
}
