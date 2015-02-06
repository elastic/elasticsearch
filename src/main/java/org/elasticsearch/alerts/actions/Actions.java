/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class Actions implements Iterable<Action>, ToXContent {

    private final List<Action> actions;

    public Actions(List<Action> actions) {
        this.actions = actions;
    }

    @Override
    public Iterator<Action> iterator() {
        return actions.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (Action action : actions){
            builder.startObject().field(action.type(), action).endObject();
        }
        builder.endArray();
        return builder;
    }

}
