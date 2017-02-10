/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public abstract class Condition implements ToXContentObject {

    protected final String type;

    protected Condition(String type) {
        this.type = type;
    }

    /**
     * @return the type of this condition
     */
    public final String type() {
        return type;
    }

    /**
     * Executes this condition
     */
    public abstract Result execute(WatchExecutionContext ctx);

    @Override
    public  XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    public static class Result implements ToXContent { // don't make this final - we can't mock final classes :(

        public Map<String,Object> getResolvedValues() {
            return resolveValues;
        }

        public enum Status {
            SUCCESS, FAILURE
        }

        private final String type;
        private final Status status;
        private final String reason;
        private final boolean met;
        @Nullable
        private final Map<String, Object> resolveValues;

        public Result(Map<String, Object> resolveValues, String type, boolean met) {
            // TODO: FAILURE status is never used, but a some code assumes that it is used
            this.status = Status.SUCCESS;
            this.type = type;
            this.met = met;
            this.reason = null;
            this.resolveValues = resolveValues;
        }

        public String type() {
            return type;
        }

        public Status status() {
            return status;
        }

        public boolean met() {
            return met;
        }

        public String reason() {
            assert status == Status.FAILURE;
            return reason;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("status", status.name().toLowerCase(Locale.ROOT));
            switch (status) {
                case SUCCESS:
                    assert reason == null;
                    builder.field("met", met);
                    break;
                case FAILURE:
                    assert reason != null && !met;
                    builder.field("reason", reason);
                    break;
                default:
                    assert false;
            }
            if (resolveValues != null) {
                builder.startObject(type).field("resolved_values", resolveValues).endObject();
            }
            return builder.endObject();
        }
    }
}
