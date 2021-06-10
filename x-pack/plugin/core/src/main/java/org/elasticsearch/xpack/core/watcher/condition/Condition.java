/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.condition;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public interface Condition extends ToXContentObject {

    /**
     * @return the type of this condition
     */
    String type();

    class Result implements ToXContentObject { // don't make this final - we can't mock final classes :(

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
                    assert reason != null && met == false;
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
