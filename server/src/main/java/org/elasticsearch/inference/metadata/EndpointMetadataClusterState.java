/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.metadata.EndpointMetadata.Display;
import org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics;
import org.elasticsearch.inference.metadata.EndpointMetadata.Internal;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.metadata.EndpointMetadata.HEURISTICS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.INTERNAL_FIELD_NAME;

/**
 * The subset of {@link EndpointMetadata} stored in cluster state. Cluster state only needs {@code heuristics} and {@code internal};
 * the remaining fields ({@code display}, {@code regions}, {@code denied_by_region_policy}) are authoritative in the {@code .inference}
 * system index and are never read back from cluster state.
 * <p>
 * The parser is intentionally lenient ({@code ignoreUnknownFields = true}) so that cluster state written by a 9.4/9.5 node — which
 * includes the full {@link EndpointMetadata} layout — is parsed correctly by a 9.6+ node that only cares about the subset.
 * <p>
 * Stream serialization writes exactly the two components ({@code heuristics} and {@code internal}); all backwards-compatibility
 * version gating for older peers is handled by {@link org.elasticsearch.inference.EndpointClusterState}, the sole consumer.
 *
 * @param heuristics contains information so clients of the Inference API can determine which models to use as defaults.
 * @param internal   contains information used internally by Elasticsearch to track when preconfigured endpoints need updating.
 */
public record EndpointMetadataClusterState(Heuristics heuristics, Internal internal) implements ToXContentObject, Writeable {

    public static final EndpointMetadataClusterState EMPTY_INSTANCE = new EndpointMetadataClusterState(
        Heuristics.EMPTY_INSTANCE,
        Internal.EMPTY_INSTANCE
    );

    private static final ConstructingObjectParser<EndpointMetadataClusterState, Void> PARSER = new ConstructingObjectParser<>(
        "endpoint_metadata_cluster_state",
        true,
        args -> new EndpointMetadataClusterState(
            args[0] == null ? Heuristics.EMPTY_INSTANCE : (Heuristics) args[0],
            args[1] == null ? Internal.EMPTY_INSTANCE : (Internal) args[1]
        )
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Heuristics.parse(p),
            new ParseField(HEURISTICS_FIELD_NAME)
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Internal.parse(p),
            new ParseField(INTERNAL_FIELD_NAME)
        );
    }

    public static EndpointMetadataClusterState parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public EndpointMetadataClusterState {
        Objects.requireNonNull(heuristics);
        Objects.requireNonNull(internal);
    }

    public EndpointMetadataClusterState(StreamInput in) throws IOException {
        this(new Heuristics(in), new Internal(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        heuristics.writeTo(out);
        internal.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(HEURISTICS_FIELD_NAME, heuristics);
        builder.field(INTERNAL_FIELD_NAME, internal);
        builder.endObject();
        return builder;
    }

    public boolean isEmpty() {
        return this.equals(EMPTY_INSTANCE);
    }

    /**
     * Creates an {@link EndpointMetadataClusterState} from the given {@link EndpointMetadata}, keeping only
     * {@code heuristics} and {@code internal}.
     */
    public static EndpointMetadataClusterState from(EndpointMetadata metadata) {
        return new EndpointMetadataClusterState(metadata.heuristics(), metadata.internal());
    }

    /**
     * Writes this subset using the full {@link EndpointMetadata} layout expected by peers older than
     * {@code inference_endpoint_metadata_cluster_state_added}. The dropped fields are written as their
     * empty/default values; their authoritative values live in the {@code .inference} system index.
     */
    public void writeAsFullEndpointMetadata(StreamOutput out) throws IOException {
        new EndpointMetadata(heuristics, internal, Display.EMPTY_INSTANCE, List.of(), false).writeTo(out);
    }

}
