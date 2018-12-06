/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.upgrade.actions;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoResponse;

public class IndexUpgradeInfoAction extends Action<IndexUpgradeInfoResponse> {

    public static final IndexUpgradeInfoAction INSTANCE = new IndexUpgradeInfoAction();
    public static final String NAME = "cluster:admin/xpack/upgrade/info";

    private IndexUpgradeInfoAction() {
        super(NAME);
    }

    @Override
    public IndexUpgradeInfoResponse newResponse() {
        return new IndexUpgradeInfoResponse();
    }

    public static class RequestBuilder
        extends MasterNodeReadOperationRequestBuilder<IndexUpgradeInfoRequest, IndexUpgradeInfoResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new IndexUpgradeInfoRequest());
        }

        public RequestBuilder setIndices(String... indices) {
            request.indices(indices);
            return this;
        }

        public RequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
            request.indicesOptions(indicesOptions);
            return this;
        }
    }
}
