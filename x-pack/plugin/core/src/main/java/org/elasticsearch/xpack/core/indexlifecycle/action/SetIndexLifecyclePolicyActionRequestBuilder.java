package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyResponse;

public class SetIndexLifecyclePolicyActionRequestBuilder
        extends ActionRequestBuilder<
        SetIndexLifecyclePolicyRequest,
        SetIndexLifecyclePolicyResponse,
        SetIndexLifecyclePolicyActionRequestBuilder> {

    public SetIndexLifecyclePolicyActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<
                    SetIndexLifecyclePolicyRequest,
                    SetIndexLifecyclePolicyResponse,
                    SetIndexLifecyclePolicyActionRequestBuilder> action) {
        super(client, action, new SetIndexLifecyclePolicyRequest());
    }

    public SetIndexLifecyclePolicyActionRequestBuilder setIndices(final String... indices) {
        request.indices(indices);
        return this;
    }

    public SetIndexLifecyclePolicyActionRequestBuilder setIndicesOptions(final IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    public SetIndexLifecyclePolicyActionRequestBuilder setPolicy(final String policy) {
        request.policy(policy);
        return this;
    }

}
