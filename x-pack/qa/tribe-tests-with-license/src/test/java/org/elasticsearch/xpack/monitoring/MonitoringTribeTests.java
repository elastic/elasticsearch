/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.client.Client;
import org.elasticsearch.license.TribeTransportTestCase;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequest;

import java.util.Collections;
import java.util.List;

public class MonitoringTribeTests extends TribeTransportTestCase {

    @Override
    protected List<String> enabledFeatures() {
        return Collections.singletonList(XPackField.MONITORING);
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
        client.execute(MonitoringBulkAction.INSTANCE, new MonitoringBulkRequest());
    }

    @Override
    protected void verifyActionOnTribeNode(Client tribeClient) {
        failAction(tribeClient, MonitoringBulkAction.INSTANCE);
    }
}
