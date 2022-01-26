/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DataStreamsStatsAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.datastreams.DataStreamClient;

import static org.hamcrest.Matchers.equalTo;

public class DataStreamTransportClientIT extends ESXPackSmokeClientTestCase {

    public void testTransportClientUsage() {
        Client client = getClient();
        XPackClient xPackClient = new XPackClient(client);
        DataStreamClient dataStreamClient = xPackClient.dataStreamClient();

        dataStreamClient.createDataStream(new CreateDataStreamAction.Request("logs-http-eu1")).actionGet();

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "logs-http-eu1" });
        GetDataStreamAction.Response response = dataStreamClient.getDataStream(getDataStreamRequest).actionGet();
        assertThat(response.getDataStreams().size(), equalTo(1));
        assertThat(response.getDataStreams().get(0).getDataStream().getName(), equalTo("logs-http-eu1"));

        DataStreamsStatsAction.Response dataStreamStatsResponse = dataStreamClient.dataStreamsStats(new DataStreamsStatsAction.Request())
            .actionGet();
        assertThat(dataStreamStatsResponse.getDataStreamCount(), equalTo(1));

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(new String[] { "logs-http-eu1" });
        dataStreamClient.deleteDataStream(deleteDataStreamRequest).actionGet();
    }

}
