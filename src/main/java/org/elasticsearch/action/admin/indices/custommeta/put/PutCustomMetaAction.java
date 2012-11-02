package org.elasticsearch.action.admin.indices.custommeta.put;

import org.elasticsearch.action.admin.indices.IndicesAction;
import org.elasticsearch.client.IndicesAdminClient;

public class PutCustomMetaAction extends IndicesAction<PutCustomMetaRequest, PutCustomMetaResponse, PutCustomMetaRequestBuilder> {

    public static final PutCustomMetaAction INSTANCE = new PutCustomMetaAction();
    public static final String NAME = "indices/custommeta/put";

    private PutCustomMetaAction() {
        super(NAME);
    }

    @Override
    public PutCustomMetaResponse newResponse() {
        return new PutCustomMetaResponse();
    }

    @Override
    public PutCustomMetaRequestBuilder newRequestBuilder(IndicesAdminClient client) {
        return new PutCustomMetaRequestBuilder(client);
    }
}