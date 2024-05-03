/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class GetInferenceStatsActionResponseTests extends AbstractBWCWireSerializationTestCase<GetInferenceStatsAction.Response> {

    public static GetInferenceStatsAction.Response createRandom() {
        return new GetInferenceStatsAction.Response(new PoolStats(randomInt(), randomInt(), randomInt(), randomInt()));
    }

    public void testToXContent() throws IOException {
        var entity = new GetInferenceStatsAction.Response(new PoolStats(1, 2, 3, 4));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"connection_pool_stats":{"leased_connections":1,"pending_connections":2,"available_connections":3,"max_connections":4}}"""));
    }

    @Override
    protected Writeable.Reader<GetInferenceStatsAction.Response> instanceReader() {
        return GetInferenceStatsAction.Response::new;
    }

    @Override
    protected GetInferenceStatsAction.Response createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInferenceStatsAction.Response mutateInstance(GetInferenceStatsAction.Response instance) throws IOException {
        var select = randomIntBetween(0, 3);
        var connPoolStats = instance.getConnectionPoolStats();

        return switch (select) {
            case 0 -> new GetInferenceStatsAction.Response(
                new PoolStats(
                    randomInt(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                )
            );
            case 1 -> new GetInferenceStatsAction.Response(
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    randomInt(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                )
            );
            case 2 -> new GetInferenceStatsAction.Response(
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    randomInt(),
                    connPoolStats.getMaxConnections()
                )
            );
            case 3 -> new GetInferenceStatsAction.Response(
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    randomInt()
                )
            );
            default -> throw new UnsupportedEncodingException(Strings.format("Encountered unsupported case %s", select));
        };
    }

    @Override
    protected GetInferenceStatsAction.Response mutateInstanceForVersion(
        GetInferenceStatsAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
