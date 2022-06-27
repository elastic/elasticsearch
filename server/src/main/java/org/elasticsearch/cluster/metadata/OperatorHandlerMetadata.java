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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Metadata class to hold the keys set in operator mode for each operator handler.
 *
 * <p>
 * Since we hold operator metadata state for multiple namespaces, the same handler can appear in
 * multiple namespaces. See {@link OperatorMetadata} and {@link Metadata}.
 */
public record OperatorHandlerMetadata(String name, Set<String> keys)
    implements
        SimpleDiffable<OperatorHandlerMetadata>,
        ToXContentFragment {

    static final ParseField KEYS = new ParseField("keys");

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(keys, StreamOutput::writeString);
    }

    /**
     * Reads an {@link OperatorHandlerMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link OperatorHandlerMetadata}
     * @throws IOException
     */
    public static OperatorHandlerMetadata readFrom(StreamInput in) throws IOException {
        return new OperatorHandlerMetadata(in.readString(), in.readSet(StreamInput::readString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.stringListField(KEYS.getPreferredName(), new TreeSet<>(keys())); // ordered set here to ensure output consistency
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<OperatorHandlerMetadata, String> PARSER = new ConstructingObjectParser<>(
        "operator_handler_metadata",
        false,
        (a, name) -> new OperatorHandlerMetadata(name, Set.copyOf((List<String>) a[0]))
    );

    static {
        PARSER.declareStringArray(optionalConstructorArg(), KEYS);
    }

    /**
     * Reads an {@link OperatorHandlerMetadata} from xContent
     *
     * @param parser {@link XContentParser}
     * @return {@link OperatorHandlerMetadata}
     * @throws IOException
     */
    public static OperatorHandlerMetadata fromXContent(XContentParser parser, String name) throws IOException {
        return PARSER.apply(parser, name);
    }

    /**
     * Reads an {@link OperatorHandlerMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link OperatorHandlerMetadata}
     * @throws IOException
     */
    public static Diff<OperatorHandlerMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(OperatorHandlerMetadata::readFrom, in);
    }
}
