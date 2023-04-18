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
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
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
 * Metadata class to hold a set of reserved keys in the cluster state, set by each {@link ReservedClusterStateHandler}.
 *
 * <p>
 * Since we hold reserved metadata state for multiple namespaces, the same handler can appear in
 * multiple namespaces. See {@link ReservedStateMetadata} and {@link Metadata}.
 */
public record ReservedStateHandlerMetadata(String name, Set<String> keys)
    implements
        SimpleDiffable<ReservedStateHandlerMetadata>,
        ToXContentFragment {

    static final ParseField KEYS = new ParseField("keys");

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(keys, StreamOutput::writeString);
    }

    /**
     * Reads an {@link ReservedStateHandlerMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link ReservedStateHandlerMetadata}
     * @throws IOException
     */
    public static ReservedStateHandlerMetadata readFrom(StreamInput in) throws IOException {
        return new ReservedStateHandlerMetadata(in.readString(), in.readSet(StreamInput::readString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.stringListField(KEYS.getPreferredName(), keys().stream().sorted().toList()); // ordered keys for output consistency
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ReservedStateHandlerMetadata, String> PARSER = new ConstructingObjectParser<>(
        "reserved_state_handler_metadata",
        false,
        (a, name) -> new ReservedStateHandlerMetadata(name, Set.copyOf((List<String>) a[0]))
    );

    static {
        PARSER.declareStringArray(optionalConstructorArg(), KEYS);
    }

    /**
     * Reads an {@link ReservedStateHandlerMetadata} from xContent
     *
     * @param parser {@link XContentParser}
     * @return {@link ReservedStateHandlerMetadata}
     * @throws IOException
     */
    public static ReservedStateHandlerMetadata fromXContent(XContentParser parser, String name) throws IOException {
        return PARSER.apply(parser, name);
    }

    /**
     * Reads an {@link ReservedStateHandlerMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link ReservedStateHandlerMetadata}
     * @throws IOException
     */
    public static Diff<ReservedStateHandlerMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ReservedStateHandlerMetadata::readFrom, in);
    }
}
