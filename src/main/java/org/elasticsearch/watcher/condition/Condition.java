/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public interface Condition extends ToXContent {

    String type();

    abstract class Result implements ToXContent {

        protected final String type;
        protected final boolean met;

        public Result(String type, boolean met) {
            this.type = type;
            this.met = met;
        }

        public String type() {
            return type;
        }

        public boolean met() { return met; }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.TYPE.getPreferredName(), type);
            builder.field(Field.MET.getPreferredName(), met);
            typeXContent(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException;
    }

    interface Builder<C extends Condition> {

        C build();
    }

    interface Field {
        ParseField TYPE = new ParseField("type");
        ParseField MET = new ParseField("met");
    }
}
