package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.master.info.ClusterInfoRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleResponse;

public class ExplainLifecycleActionRequestBuilder
        extends ClusterInfoRequestBuilder<ExplainLifecycleRequest, ExplainLifecycleResponse, ExplainLifecycleActionRequestBuilder> {

    public ExplainLifecycleActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<ExplainLifecycleRequest, ExplainLifecycleResponse, ExplainLifecycleActionRequestBuilder> action) {
        super(client, action, new ExplainLifecycleRequest());
    }

}
