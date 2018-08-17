package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.indexlifecycle.StartILMRequest;

public class StartILMActionRequestBuilder
        extends ActionRequestBuilder<StartILMRequest, AcknowledgedResponse, StartILMActionRequestBuilder> {

    public StartILMActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<StartILMRequest, AcknowledgedResponse, StartILMActionRequestBuilder> action) {
        super(client, action, new StartILMRequest());
    }

}
