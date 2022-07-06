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
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Metadata class to hold a set of immutable keys in the cluster state, set by each {@link ImmutableClusterStateHandler}.
 *
 * <p>
 * Since we hold immutable metadata state for multiple namespaces, the same handler can appear in
 * multiple namespaces. See {@link ImmutableStateMetadata} and {@link Metadata}.
 */
public record ImmutableStateHandlerMetadata(String name, Set<String> keys)
    implements
        SimpleDiffable<ImmutableStateHandlerMetadata>,
        ToXContentFragment {

    static final ParseField KEYS = new ParseField("keys");

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(keys, StreamOutput::writeString);
    }

    /**
     * Reads an {@link ImmutableStateHandlerMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link ImmutableStateHandlerMetadata}
     * @throws IOException
     */
    public static ImmutableStateHandlerMetadata readFrom(StreamInput in) throws IOException {
        return new ImmutableStateHandlerMetadata(in.readString(), in.readSet(StreamInput::readString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.stringListField(KEYS.getPreferredName(), keys().stream().sorted().toList()); // ordered keys for output consistency
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ImmutableStateHandlerMetadata, String> PARSER = new ConstructingObjectParser<>(
        "immutable_state_handler_metadata",
        false,
        (a, name) -> new ImmutableStateHandlerMetadata(name, Set.copyOf((List<String>) a[0]))
    );

    static {
        PARSER.declareStringArray(optionalConstructorArg(), KEYS);
    }

    /**
     * Reads an {@link ImmutableStateHandlerMetadata} from xContent
     *
     * @param parser {@link XContentParser}
     * @return {@link ImmutableStateHandlerMetadata}
     * @throws IOException
     */
    public static ImmutableStateHandlerMetadata fromXContent(XContentParser parser, String name) throws IOException {
        return PARSER.apply(parser, name);
    }

    /**
     * Reads an {@link ImmutableStateHandlerMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link ImmutableStateHandlerMetadata}
     * @throws IOException
     */
    public static Diff<ImmutableStateHandlerMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ImmutableStateHandlerMetadata::readFrom, in);
    }
}
