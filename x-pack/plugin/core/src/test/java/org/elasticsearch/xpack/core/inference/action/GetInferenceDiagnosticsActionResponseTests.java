/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.List;

public class GetInferenceDiagnosticsActionResponseTests extends AbstractWireSerializingTestCase<GetInferenceDiagnosticsAction.Response> {

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
        var poolStats = new PoolStats(1, 2, 3, 4);
        var entity = new GetInferenceDiagnosticsAction.Response(
            ClusterName.DEFAULT,
            List.of(new GetInferenceDiagnosticsAction.NodeResponse(node, poolStats)),
            List.of()
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"id":{"connection_pool_stats":{"leased_connections":1,"pending_connections":2,"available_connections":3,""" + """
            "max_connections":4}}}"""));
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
}
