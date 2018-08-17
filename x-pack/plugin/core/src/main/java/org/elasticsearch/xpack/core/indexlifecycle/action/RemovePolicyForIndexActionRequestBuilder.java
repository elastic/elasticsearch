package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public class RemovePolicyForIndexActionRequestBuilder
        extends ActionRequestBuilder<
        RemovePolicyForIndexAction.Request,
        RemovePolicyForIndexAction.Response,
        RemovePolicyForIndexActionRequestBuilder> {

    public RemovePolicyForIndexActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<
                    RemovePolicyForIndexAction.Request,
                    RemovePolicyForIndexAction.Response,
                    RemovePolicyForIndexActionRequestBuilder> action) {
        super(client, action, new RemovePolicyForIndexAction.Request());
    }

    public RemovePolicyForIndexActionRequestBuilder setIndices(final String... indices) {
        request.indices(indices);
        return this;
    }

    public RemovePolicyForIndexActionRequestBuilder setIndicesOptions(final IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

}
