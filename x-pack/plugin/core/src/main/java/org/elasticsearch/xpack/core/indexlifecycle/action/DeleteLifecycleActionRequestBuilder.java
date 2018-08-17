package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class DeleteLifecycleActionRequestBuilder extends ActionRequestBuilder<DeleteLifecycleAction.Request, DeleteLifecycleAction.Response, DeleteLifecycleActionRequestBuilder> {

    public DeleteLifecycleActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<DeleteLifecycleAction.Request, DeleteLifecycleAction.Response, DeleteLifecycleActionRequestBuilder> action) {
        super(client, action, new DeleteLifecycleAction.Request());
    }

    public DeleteLifecycleActionRequestBuilder setPolicyName(final String policyName) {
        request.setPolicyName(policyName);
        return this;
    }

}
