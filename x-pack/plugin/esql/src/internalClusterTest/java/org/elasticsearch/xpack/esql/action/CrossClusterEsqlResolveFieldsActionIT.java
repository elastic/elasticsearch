/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.hamcrest.Matchers;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CrossClusterEsqlResolveFieldsActionIT extends AbstractCrossClusterTestCase {

    public void testResolveIndexAbstractions() {
        // indices
        client(LOCAL_CLUSTER).admin().indices().prepareCreate("local-index").setMapping("f1", "type=keyword", "f2", "type=long").get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareCreate("remote-index").setMapping("f1", "type=keyword", "f3", "type=long").get();

        // views
        assertAcked(
            client(LOCAL_CLUSTER).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS,
                    new View("local-view", "FROM local-index | WHERE true")
                )
            ).actionGet(30, TimeUnit.SECONDS)
        );
        assertAcked(
            client(REMOTE_CLUSTER_1).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS,
                    new View("remote-view", "FROM remote-index | WHERE true")
                )
            ).actionGet(30, TimeUnit.SECONDS)
        );

        // external data sets
        // TODO

        // resolve schema
        var request = new EsqlResolveFieldsRequest(
            new FieldCapabilitiesRequest().indices("*", "*:*").fields("*").indicesOptions(IndexResolver.DEFAULT_OPTIONS),
            false, // permit remote views and data sets
            true // resolve index abstractions
        );
        var response = client(LOCAL_CLUSTER).execute(EsqlResolveFieldsAction.TYPE, request).actionGet(30, TimeUnit.SECONDS);
        assertThat(
            response.resolvedIndexAbstractions(),
            Matchers.containsInAnyOrder(
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("local-index", IndexAbstraction.Type.CONCRETE_INDEX),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("cluster-a:remote-index", IndexAbstraction.Type.CONCRETE_INDEX),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("local-view", IndexAbstraction.Type.VIEW),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("cluster-a:remote-view", IndexAbstraction.Type.VIEW)
            )
        );
    }
}
