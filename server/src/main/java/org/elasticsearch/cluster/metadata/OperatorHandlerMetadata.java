/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Metadata class to hold the operator set keys for each operator handler
 *
 */
public class OperatorHandlerMetadata implements SimpleDiffable<OperatorHandlerMetadata>, ToXContentFragment {
    private final String name;
    private final Set<String> keys;

    public OperatorHandlerMetadata(String name, Set<String> keys) {
        this.name = name;
        this.keys = keys;
    }

    public String name() {
        return this.name;
    }

    public Set<String> keys() {
        return this.keys;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(keys, StreamOutput::writeString);
    }

    public static OperatorHandlerMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString()).keys(in.readSet(StreamInput::readString));
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    public static Diff<OperatorHandlerMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(OperatorHandlerMetadata::readFrom, in);
    }

    public static class Builder {
        private static final String HANDLER_KEYS = "keys";

        private final String name;
        private Set<String> keys;

        public Builder(String name) {
            this.name = name;
            this.keys = new HashSet<>();
        }

        public Builder keys(Set<String> keys) {
            this.keys = keys;
            return this;
        }

        public OperatorHandlerMetadata build() {
            return new OperatorHandlerMetadata(name, Collections.unmodifiableSet(keys));
        }

        /**
         * Serializes the metadata to xContent
         *
         * @param metadata
         * @param builder
         * @param params
         */
        public static void toXContent(OperatorHandlerMetadata metadata, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
            builder.startObject(metadata.name());
            builder.stringListField(HANDLER_KEYS, metadata.keys);
            builder.endObject();
        }

        /**
         * Reads the metadata from xContent
         *
         * @param parser
         * @return
         * @throws IOException
         */
        public static OperatorHandlerMetadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = parser.currentName();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (HANDLER_KEYS.equals(currentFieldName)) {
                        Set<String> handlerKeys = new HashSet<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            handlerKeys.add(parser.text());
                        }
                        builder.keys(handlerKeys);
                    }
                }
            }
            return builder.build();
        }
    }
}
