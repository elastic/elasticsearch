/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.index;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class IndexAlertResponse extends ActionResponse {
    private IndexResponse indexResponse;

    public IndexAlertResponse(IndexResponse indexResponse) {
        this.indexResponse = indexResponse;
    }

    public IndexAlertResponse() {
        indexResponse = null;
    }


    public IndexResponse indexResponse(){
        return indexResponse;
    }

    public void indexResponse(IndexResponse indexResponse){
        this.indexResponse = indexResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(indexResponse != null);
        if (indexResponse != null) {
            indexResponse.writeTo(out);
        }
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            indexResponse = new IndexResponse();
            indexResponse.readFrom(in);
        }
    }
}
