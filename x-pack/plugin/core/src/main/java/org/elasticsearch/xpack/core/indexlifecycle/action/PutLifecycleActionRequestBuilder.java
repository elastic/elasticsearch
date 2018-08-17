package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;

public class PutLifecycleActionRequestBuilder
        extends ActionRequestBuilder<PutLifecycleAction.Request, PutLifecycleAction.Response, PutLifecycleActionRequestBuilder> {

    public PutLifecycleActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<PutLifecycleAction.Request, PutLifecycleAction.Response, PutLifecycleActionRequestBuilder> action) {
        super(client, action, new PutLifecycleAction.Request());
    }

    public PutLifecycleActionRequestBuilder setPolicy(LifecyclePolicy policy) {
        request.setPolicy(policy);
        return this;
    }

}
