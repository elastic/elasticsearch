/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;

public class PutStoredScriptRequestBuilder extends AcknowledgedRequestBuilder<
    PutStoredScriptRequest,
    AcknowledgedResponse,
    PutStoredScriptRequestBuilder> {

    public PutStoredScriptRequestBuilder(ElasticsearchClient client, PutStoredScriptAction action) {
        super(client, action, new PutStoredScriptRequest());
    }

    public PutStoredScriptRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    public PutStoredScriptRequestBuilder setContext(String context) {
        request.context(context);
        return this;
    }

    /**
     * Set the source of the script along with the content type of the source
     */
    public PutStoredScriptRequestBuilder setContent(BytesReference source, XContentType xContentType) {
        request.content(source, xContentType);
        return this;
    }
}
