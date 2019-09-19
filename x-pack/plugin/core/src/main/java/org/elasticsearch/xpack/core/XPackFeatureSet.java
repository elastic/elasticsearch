/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public interface XPackFeatureSet {

    String name();

    boolean available();

    boolean enabled();

    abstract class Usage implements ToXContentObject, NamedWriteable {

        private static final String AVAILABLE_XFIELD = "available";
        private static final String ENABLED_XFIELD = "enabled";

        protected final String name;
        protected final boolean available;
        protected final boolean enabled;

        public Usage(StreamInput input) throws IOException {
            this(input.readString(), input.readBoolean(), input.readBoolean());
        }

        public Usage(String name, boolean available, boolean enabled) {
            this.name = name;
            this.available = available;
            this.enabled = enabled;
        }

        public String name() {
            return name;
        }

        public boolean available() {
            return available;
        }

        public boolean enabled() {
            return enabled;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeBoolean(available);
            out.writeBoolean(enabled);
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerXContent(builder, params);
            return builder.endObject();
        }

        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(AVAILABLE_XFIELD, available);
            builder.field(ENABLED_XFIELD, enabled);
        }
    }

}
