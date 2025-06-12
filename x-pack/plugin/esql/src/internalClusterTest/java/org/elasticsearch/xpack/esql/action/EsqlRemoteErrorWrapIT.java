/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import static org.hamcrest.Matchers.is;

public class EsqlRemoteErrorWrapIT extends AbstractCrossClusterTestCase {

    public void testThatRemoteErrorsAreWrapped() throws Exception {
        setupClusters(2);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        /*
         * Let's say something went wrong with the Exchange and its specifics when talking to a remote.
         * And let's pretend only cluster-a is affected.
         */
        for (var nodes : cluster(REMOTE_CLUSTER_1).getNodeNames()) {
            ((MockTransportService) cluster(REMOTE_CLUSTER_1).getInstance(TransportService.class, nodes)).addRequestHandlingBehavior(
                ExchangeService.OPEN_EXCHANGE_ACTION_NAME,
                (requestHandler, transportRequest, transportChannel, transportTask) -> {
                    throw new IllegalArgumentException("some error to wreck havoc");
                }
            );
        }

        RemoteException wrappedError = expectThrows(
            RemoteException.class,
            () -> runQuery("FROM " + REMOTE_CLUSTER_1 + ":*," + REMOTE_CLUSTER_2 + ":* | LIMIT 100", false)
        );
        assertThat(wrappedError.getMessage(), is("Remote [cluster-a] encountered an error"));
    }
}
