/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class IndexAlertAction implements AlertAction, ToXContent {
    private final String index;
    private final String type;

    public IndexAlertAction(String index, String type){
        this.index = index;
        this.type = type;
    }

    @Override
    public String getActionName() {
        return "index";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("type", type);
        builder.endObject();
        return builder;
    }

    public String getType() {
        return type;
    }

    public String getIndex() {
        return index;
    }

}
