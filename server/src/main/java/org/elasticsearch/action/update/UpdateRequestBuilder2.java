/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.RequestBuilder;

public class UpdateRequestBuilder2 implements RequestBuilder<UpdateRequest> {
    private final String index;
    private final String id;
    private String routing;
    private String script;
    public UpdateRequestBuilder2(String index, String id) {
        this.index = index;
        this.id = id;
    }

    public void routing(String routing) {
        this.routing = routing;
    }

    public void script(String script) {
        this.script = script;
    }

    @Override
    public UpdateRequest buildRequest() {
        UpdateRequest updateRequest = new UpdateRequest(index, id);
        try {
            if (routing != null) {
                updateRequest.routing(routing);
            }
            if (script != null) {
                updateRequest.script(script);
            }
            return updateRequest;
        } catch (Exception e) {
            updateRequest.decRef();
            throw e;
        }
    }
}
