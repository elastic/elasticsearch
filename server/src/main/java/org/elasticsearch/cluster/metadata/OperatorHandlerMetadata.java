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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata class to hold the keys set in operator mode for each operator handler. Since we
 * hold operator metadata state for multiple namespaces, the same handler can appear in
 * multiple namespaces. See {@link OperatorMetadata} and {@link Metadata}.
 */
public record OperatorHandlerMetadata(String name, Set<String> keys)
    implements
        SimpleDiffable<OperatorHandlerMetadata>,
        ToXContentFragment {

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(keys, StreamOutput::writeString);
    }

    /**
     * Reads an {@link OperatorHandlerMetadata} from a {@link StreamInput}
     * @param in the {@link StreamInput} to read from
     * @return {@link OperatorHandlerMetadata}
     * @throws IOException
     */
    public static OperatorHandlerMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString()).keys(in.readSet(StreamInput::readString));
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder);
        return builder;
    }

    /**
     * Reads an {@link OperatorHandlerMetadata} {@link Diff} from {@link StreamInput}
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link OperatorHandlerMetadata}
     * @throws IOException
     */
    public static Diff<OperatorHandlerMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(OperatorHandlerMetadata::readFrom, in);
    }

    /**
     * Builder class for {@link OperatorHandlerMetadata}
     */
    public static class Builder {
        private static final String HANDLER_KEYS = "keys";

        private final String name;
        private Set<String> keys;

        /**
         * Default constructor, the handler name is a required parameter
         * @param name the handler name
         */
        public Builder(String name) {
            this.name = name;
            this.keys = new HashSet<>();
        }

        /**
         * Set the cluster state keys recorded/written by this handler in the cluster state
         * @param keys a set of String keys of the state that's written by this handler
         * @return {@link Builder}
         */
        public Builder keys(Set<String> keys) {
            this.keys = keys;
            return this;
        }

        /**
         * Builds the {@link OperatorHandlerMetadata}
         * @return {@link OperatorHandlerMetadata}
         */
        public OperatorHandlerMetadata build() {
            return new OperatorHandlerMetadata(name, Collections.unmodifiableSet(keys));
        }

        /**
         * Serializes an {@link OperatorHandlerMetadata} to xContent
         *
         * @param metadata {@link OperatorHandlerMetadata}
         * @param builder {@link XContentBuilder}
         */
        public static void toXContent(OperatorHandlerMetadata metadata, XContentBuilder builder) throws IOException {
            builder.startObject(metadata.name());
            builder.stringListField(HANDLER_KEYS, new TreeSet<>(metadata.keys)); // ordered set here to ensure output consistency
            builder.endObject();
        }

        /**
         * Reads an {@link OperatorHandlerMetadata} from xContent
         *
         * @param parser {@link XContentParser}
         * @return {@link OperatorHandlerMetadata}
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
