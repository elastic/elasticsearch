/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

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

    public int count() {
        return actions.size();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Actions actions1 = (Actions) o;

        if (!actions.equals(actions1.actions)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return actions.hashCode();
    }
}
