package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class GetStatusActionRequestBuilder
        extends ActionRequestBuilder<GetStatusAction.Request, GetStatusAction.Response, GetStatusActionRequestBuilder> {

    public GetStatusActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<GetStatusAction.Request, GetStatusAction.Response, GetStatusActionRequestBuilder> action) {
        super(client, action, new GetStatusAction.Request());
    }

}
