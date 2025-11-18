/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class GetInferenceDiagnosticsActionResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceDiagnosticsAction.Response> {

    public static GetInferenceDiagnosticsAction.Response createRandom() {
        List<GetInferenceDiagnosticsAction.NodeResponse> responses = randomList(
            2,
            10,
            GetInferenceDiagnosticsActionNodeResponseTests::createRandom
        );
        return new GetInferenceDiagnosticsAction.Response(ClusterName.DEFAULT, responses, List.of());
    }

    public void testToXContent() throws IOException {
        var node = DiscoveryNodeUtils.create("id");
        var externalPoolStats = new PoolStats(1, 2, 3, 4);
        var eisPoolStats = new PoolStats(5, 6, 7, 8);
        var entity = new GetInferenceDiagnosticsAction.Response(
            ClusterName.DEFAULT,
            List.of(
                new GetInferenceDiagnosticsAction.NodeResponse(
                    node,
                    externalPoolStats,
                    eisPoolStats,
                    new GetInferenceDiagnosticsAction.NodeResponse.Stats(5, 6, 7, 8)
                )
            ),
            List.of()
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "id":{
                    "external": {
                        "connection_pool_stats":{
                            "leased_connections":1,
                            "pending_connections":2,
                            "available_connections":3,
                            "max_connections":4
                        }
                    },
                    "eis_mtls": {
                        "connection_pool_stats":{
                            "leased_connections":5,
                            "pending_connections":6,
                            "available_connections":7,
                            "max_connections":8
                        }
                    },
                    "inference_endpoint_registry":{
                        "cache_count": 5,
                        "cache_hits": 6,
                        "cache_misses": 7,
                        "cache_evictions": 8
                    }
                }
            }""")));
    }

    @Override
    protected Writeable.Reader<GetInferenceDiagnosticsAction.Response> instanceReader() {
        return GetInferenceDiagnosticsAction.Response::new;
    }

    @Override
    protected GetInferenceDiagnosticsAction.Response createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInferenceDiagnosticsAction.Response mutateInstance(GetInferenceDiagnosticsAction.Response instance) {
        return new GetInferenceDiagnosticsAction.Response(
            ClusterName.DEFAULT,
            instance.getNodes().subList(1, instance.getNodes().size()),
            List.of()
        );
    }

    @Override
    protected GetInferenceDiagnosticsAction.Response mutateInstanceForVersion(
        GetInferenceDiagnosticsAction.Response instance,
        TransportVersion version
    ) {
        return new GetInferenceDiagnosticsAction.Response(
            instance.getClusterName(),
            instance.getNodes()
                .stream()
                .map(nodeResponse -> GetInferenceDiagnosticsActionNodeResponseTests.mutateNodeResponseForVersion(nodeResponse, version))
                .toList(),
            instance.failures()
        );
    }
}
