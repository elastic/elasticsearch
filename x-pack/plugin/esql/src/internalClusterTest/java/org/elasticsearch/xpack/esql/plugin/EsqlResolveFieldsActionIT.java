/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction.IndexAbstractionSchema;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class EsqlResolveFieldsActionIT extends AbstractCrossClusterTestCase {

    public void testSchemaIncludesLocalAndRemoteIndices() {
        client(LOCAL_CLUSTER).admin().indices().prepareCreate("local-index").setMapping("local_field", "type=keyword").get();
        client(LOCAL_CLUSTER).prepareIndex("local-index").setSource("local_field", "value").get();
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh("local-index").get();

        client(REMOTE_CLUSTER_1).admin().indices().prepareCreate("remote-index").setMapping("remote_field", "type=keyword").get();
        client(REMOTE_CLUSTER_1).prepareIndex("remote-index").setSource("remote_field", "value").get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh("remote-index").get();

        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices("local-index", REMOTE_CLUSTER_1 + ":remote-index");
        request.fields("local_field", "remote_field");
        request.includeUnmapped(false);
        request.returnLocalAll(false);
        request.setMergeResults(false);
        request.indicesOptions(IndicesOptions.strictExpand());

        EsqlResolveFieldsResponse response = client(LOCAL_CLUSTER).execute(EsqlResolveFieldsAction.TYPE, request)
            .actionGet(30, TimeUnit.SECONDS);

        // Remote schema is keyed by the original cross-cluster expression; each index reports only its own fields
        assertThat(
            response.schema(),
            equalTo(
                Map.of(
                    "local-index",
                    List.of(
                        new IndexAbstractionSchema("local-index", IndexAbstraction.Type.CONCRETE_INDEX, Map.of("local_field", "keyword"))
                    ),
                    REMOTE_CLUSTER_1 + ":remote-index",
                    List.of(
                        new IndexAbstractionSchema("remote-index", IndexAbstraction.Type.CONCRETE_INDEX, Map.of("remote_field", "keyword"))
                    )
                )
            )
        );
    }
}
