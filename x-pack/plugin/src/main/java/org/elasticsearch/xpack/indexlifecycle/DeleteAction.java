/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;

public class DeleteAction extends Action {
    private static final Logger logger = ESLoggerFactory.getLogger(DeleteAction.class);

    public static final ParseField INDEX_FIELD = new ParseField("index");

    private Index index;

    public DeleteAction(Index index) {
        this.index = index;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index.getName());
        builder.endObject();
        return builder;
    }

    @Override
    protected void execute(Client client) {
        client.admin().indices().prepareDelete(index.getName()).execute(new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                logger.error(deleteIndexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
            }
        });
    }

}
