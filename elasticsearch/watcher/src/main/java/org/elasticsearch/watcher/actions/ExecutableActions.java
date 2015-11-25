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
import java.util.Map;

/**
 *
 */
public class ExecutableActions implements Iterable<ActionWrapper>, ToXContent {

    private final List<ActionWrapper> actions;

    public ExecutableActions(List<ActionWrapper> actions) {
        this.actions = actions;
    }

    public int count() {
        return actions.size();
    }

    @Override
    public Iterator<ActionWrapper> iterator() {
        return actions.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (ActionWrapper action : actions) {
            builder.field(action.id(), action, params);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutableActions actions1 = (ExecutableActions) o;

        if (!actions.equals(actions1.actions)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return actions.hashCode();
    }

    public static class Results implements Iterable<ActionWrapper.Result>, ToXContent {

        private final Map<String, ActionWrapper.Result> results;

        public Results(Map<String, ActionWrapper.Result> results) {
            this.results = results;
        }

        public int count() {
            return results.size();
        }

        @Override
        public Iterator<ActionWrapper.Result> iterator() {
            return results.values().iterator();
        }

        public ActionWrapper.Result get(String id) {
            return results.get(id);
        }

        public boolean throttled() {
            for (ActionWrapper.Result result : results.values()) {
                if (result.action().status() == Action.Result.Status.THROTTLED) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Results results1 = (Results) o;

            return results.equals(results1.results);
        }

        @Override
        public int hashCode() {
            return results.hashCode();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (ActionWrapper.Result result : results.values()) {
                result.toXContent(builder, params);
            }
            return builder.endArray();
        }
    }
}
