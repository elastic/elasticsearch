/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class IndexResolverTests extends ESTestCase {

    /**
     * The original indices of a derived pattern of data streams are the data streams of the backing indices field caps matched, per
     * cluster and without duplicates; requested data streams that matched nothing (here {@code exemplars-k8s}) are absent. A matched
     * index that is not a data stream backing index is kept under its own name.
     */
    public void testMatchedDataStreams() {
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.builder()
            .withIndexResponses(
                List.of(
                    indexResponse(".ds-exemplars-cpu-2026.09.01-000001"),
                    indexResponse(".ds-exemplars-cpu-2026.09.08-000002"),
                    indexResponse("exemplars-legacy"),
                    indexResponse("remote:.ds-exemplars-generic.otel-default-2026.09.08-000001")
                )
            )
            .build();

        Map<String, List<String>> originalIndices = IndexResolver.MATCHED_DATA_STREAMS.apply(
            "exemplars-cpu,exemplars-k8s,exemplars-legacy,remote:exemplars-generic.otel-default",
            response
        );

        assertEquals(
            Map.of("", List.of("exemplars-cpu", "exemplars-legacy"), "remote", List.of("exemplars-generic.otel-default")),
            originalIndices
        );
    }

    public void testMatchedDataStreamsWithoutMatches() {
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.builder().withIndexResponses(List.of()).build();
        assertEquals(Map.of(), IndexResolver.MATCHED_DATA_STREAMS.apply("exemplars-cpu", response));
    }

    private static FieldCapabilitiesIndexResponse indexResponse(String index) {
        return new FieldCapabilitiesIndexResponse(index, null, Map.of(), true, IndexMode.TIME_SERIES, 1);
    }
}
