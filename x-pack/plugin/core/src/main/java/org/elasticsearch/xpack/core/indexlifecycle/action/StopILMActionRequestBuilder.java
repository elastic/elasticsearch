package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.indexlifecycle.StopILMRequest;

public class StopILMActionRequestBuilder extends ActionRequestBuilder<StopILMRequest, AcknowledgedResponse, StopILMActionRequestBuilder> {

    public StopILMActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<StopILMRequest, AcknowledgedResponse, StopILMActionRequestBuilder> action) {
        super(client, action, new StopILMRequest());
    }

}
