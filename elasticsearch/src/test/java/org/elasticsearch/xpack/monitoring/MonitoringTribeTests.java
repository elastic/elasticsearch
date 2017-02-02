/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.TribeTransportTestCase;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkRequest;

import java.util.Collections;
import java.util.List;

public class MonitoringTribeTests extends TribeTransportTestCase {

    @Override
    protected List<String> enabledFeatures() {
        return Collections.singletonList(Monitoring.NAME);
    }

    @Override
    protected void verifyActionOnClientNode(Client client) throws Exception {
        assertMonitoringTransportActionsWorks(client);
    }

    @Override
    protected void verifyActionOnMasterNode(Client masterClient) throws Exception {
        assertMonitoringTransportActionsWorks(masterClient);
    }

    @Override
    protected void verifyActionOnDataNode(Client dataNodeClient) throws Exception {
        assertMonitoringTransportActionsWorks(dataNodeClient);
    }

    private static void assertMonitoringTransportActionsWorks(Client client) throws Exception {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(randomAsciiOfLength(2), randomAsciiOfLength(2));
        doc.setType(randomAsciiOfLength(5));
        doc.setSource(new BytesArray("{\"key\" : \"value\"}"), XContentType.JSON);
        client.execute(MonitoringBulkAction.INSTANCE, new MonitoringBulkRequest());
    }

    @Override
    protected void verifyActionOnTribeNode(Client tribeClient) {
        failAction(tribeClient, MonitoringBulkAction.INSTANCE);
    }
}
