package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class ClearCcrRestoreSessionRequestBuilder extends ActionRequestBuilder<ClearCcrRestoreSessionRequest,
    ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse, ClearCcrRestoreSessionRequestBuilder> {

    ClearCcrRestoreSessionRequestBuilder(ElasticsearchClient client) {
        super(client, ClearCcrRestoreSessionAction.INSTANCE, new ClearCcrRestoreSessionRequest());
    }
}
