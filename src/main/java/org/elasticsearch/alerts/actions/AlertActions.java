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
public class AlertActions implements Iterable<AlertAction>, ToXContent {

    private final List<AlertAction> actions;

    public AlertActions(List<AlertAction> actions) {
        this.actions = actions;
    }

    @Override
    public Iterator<AlertAction> iterator() {
        return actions.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (AlertAction action : actions){
            builder.field(action.getActionName());
            action.toXContent(builder, params);
        }
        return builder.endObject();
    }
}
