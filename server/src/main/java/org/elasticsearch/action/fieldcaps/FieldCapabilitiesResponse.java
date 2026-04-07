/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest.RESOLVED_FIELDS_CAPS;

/**
 * Response for {@link FieldCapabilitiesRequest} requests.
 */
public class FieldCapabilitiesResponse extends ActionResponse implements ChunkedToXContentObject {

    private static final TransportVersion MIN_TRANSPORT_VERSION = TransportVersion.fromName("min_transport_version");

    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField FAILED_INDICES_FIELD = new ParseField("failed_indices");
    public static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final String[] indices;

    // Index expressions resolved in the context of the node that creates this response.
    // If created on a linked project, "local" refers to that linked project.
    // If created on the coordinating node, "local" refers to the coordinator itself.
    // This data is sent from linked projects to the coordinator to inform it of each remote's local resolution state.
    private final ResolvedIndexExpressions resolvedLocally;
    // Remotely resolved index expressions, keyed by project alias.
    // This is only populated by the coordinating node with the `resolvedLocally` data structure it receives
    // back from the remotes. Used in the coordinating node for error checking, it's never sent over the wire.
    // Keeping this distinction (between resolvedLocally and resolvedRemotely) further prevents project chaining
    // and simplifies resolution logic, because the remoteExpressions in the resolvedLocally data structure are
    // used to access data in `resolvedRemotely`.
    private final transient Map<String, ResolvedIndexExpressions> resolvedRemotely;
    private final Map<String, Map<String, FieldCapabilities>> fields;
    private final List<FieldCapabilitiesFailure> failures;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;
    private final TransportVersion minTransportVersion;

    public FieldCapabilitiesResponse(
        String[] indices,
        Map<String, Map<String, FieldCapabilities>> fields,
        List<FieldCapabilitiesFailure> failures
    ) {
        this(indices, null, Collections.emptyMap(), fields, Collections.emptyList(), failures, null);
    }

    public FieldCapabilitiesResponse(String[] indices, Map<String, Map<String, FieldCapabilities>> fields) {
        this(indices, null, Collections.emptyMap(), fields, Collections.emptyList(), Collections.emptyList(), null);
    }

    public static FieldCapabilitiesResponse empty() {
        return new FieldCapabilitiesResponse(
            Strings.EMPTY_ARRAY,
            null,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyList(),
            null
        );
    }

    public FieldCapabilitiesResponse(List<FieldCapabilitiesIndexResponse> indexResponses, List<FieldCapabilitiesFailure> failures) {
        this(Strings.EMPTY_ARRAY, null, Collections.emptyMap(), Collections.emptyMap(), indexResponses, failures, null);
    }

    public static FieldCapabilitiesResponse.Builder builder() {
        return new FieldCapabilitiesResponse.Builder();
    }

    private FieldCapabilitiesResponse(
        String[] indices,
        ResolvedIndexExpressions resolvedLocally,
        Map<String, ResolvedIndexExpressions> resolvedRemotely,
        Map<String, Map<String, FieldCapabilities>> fields,
        List<FieldCapabilitiesIndexResponse> indexResponses,
        List<FieldCapabilitiesFailure> failures,
        TransportVersion minTransportVersion
    ) {
        this.fields = Objects.requireNonNull(fields);
        this.resolvedLocally = resolvedLocally;
        this.resolvedRemotely = Objects.requireNonNull(resolvedRemotely);
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.indices = indices;
        this.failures = failures;
        this.minTransportVersion = minTransportVersion;
    }

    public FieldCapabilitiesResponse(StreamInput in) throws IOException {
        this.indices = in.readStringArray();
        this.fields = in.readMap(FieldCapabilitiesResponse::readField);
        this.indexResponses = FieldCapabilitiesIndexResponse.readList(in);
        this.failures = in.readCollectionAsList(FieldCapabilitiesFailure::new);
        this.minTransportVersion = in.getTransportVersion().supports(MIN_TRANSPORT_VERSION)
            ? in.readOptional(TransportVersion::readVersion)
            : null;
        if (in.getTransportVersion().supports(RESOLVED_FIELDS_CAPS)) {
            this.resolvedLocally = in.readOptionalWriteable(ResolvedIndexExpressions::new);
        } else {
            this.resolvedLocally = null;
        }
        // when receiving a response we expect the resolved remotely to be empty.
        // It's only non-empty on the coordinating node if the FC requests targets remotes.
        this.resolvedRemotely = Collections.emptyMap();
    }

    /**
     * Get the concrete list of indices that were requested and returned a response.
     */
    public String[] getIndices() {
        return indices;
    }

    /**
     * Get the concrete list of indices that failed
     */
    public int getFailedIndicesCount() {
        int count = 0;
        for (FieldCapabilitiesFailure fieldCapabilitiesFailure : this.failures) {
            int length = fieldCapabilitiesFailure.getIndices().length;
            count += length;
        }
        return count;
    }

    /**
     * Get the field capabilities map.
     */
    public Map<String, Map<String, FieldCapabilities>> get() {
        return fields;
    }

    /**
     * Get possible request failures keyed by index name
     */
    public List<FieldCapabilitiesFailure> getFailures() {
        return failures;
    }

    /**
     * Returns the actual per-index field caps responses
     */
    public List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    /**
     * Locally resolved index expressions
     */
    public ResolvedIndexExpressions getResolvedLocally() {
        return resolvedLocally;
    }

    /**
     * Remotely resolved index expressions, non-empty only in the FC coordinator
     */
    public Map<String, ResolvedIndexExpressions> getResolvedRemotely() {
        return resolvedRemotely;
    }

    /**
     * Get the field capabilities per type for the provided {@code field}.
     */
    public Map<String, FieldCapabilities> getField(String field) {
        return fields.get(field);
    }

    /**
     * @return the minTransportVersion across all clusters involved in resolution
     */
    @Nullable
    public TransportVersion minTransportVersion() {
        return minTransportVersion;
    }

    /**
     * Build a new response replacing the {@link #minTransportVersion()}.
     */
    public FieldCapabilitiesResponse withMinTransportVersion(TransportVersion newMin) {
        return new FieldCapabilitiesResponse(indices, resolvedLocally, resolvedRemotely, fields, indexResponses, failures, newMin);
    }

    /**
     * Returns <code>true</code> if the provided field is a metadata field.
     */
    public boolean isMetadataField(String field) {
        Map<String, FieldCapabilities> caps = getField(field);
        if (caps == null) {
            return false;
        }
        return caps.values().stream().anyMatch(FieldCapabilities::isMetadataField);
    }

    private static Map<String, FieldCapabilities> readField(StreamInput in) throws IOException {
        return in.readMap(FieldCapabilities::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeMap(fields, FieldCapabilitiesResponse::writeField);
        FieldCapabilitiesIndexResponse.writeList(out, indexResponses);
        out.writeCollection(failures);
        if (out.getTransportVersion().supports(MIN_TRANSPORT_VERSION)) {
            out.writeOptional((Writer<TransportVersion>) (o, v) -> TransportVersion.writeVersion(v, o), minTransportVersion);
        }
        if (out.getTransportVersion().supports(RESOLVED_FIELDS_CAPS)) {
            out.writeOptionalWriteable(resolvedLocally);
        }
    }

    private static void writeField(StreamOutput out, Map<String, FieldCapabilities> map) throws IOException {
        out.writeMap(map, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        if (indexResponses.size() > 0) {
            throw new IllegalStateException("cannot serialize non-merged response");
        }

        return Iterators.concat(
            Iterators.single(
                (b, p) -> b.startObject().array(INDICES_FIELD.getPreferredName(), indices).startObject(FIELDS_FIELD.getPreferredName())
            ),
            Iterators.map(fields.entrySet().iterator(), r -> (b, p) -> b.xContentValuesMap(r.getKey(), r.getValue())),
            this.failures.size() > 0
                ? Iterators.concat(
                    Iterators.single(
                        (ToXContent) (b, p) -> b.endObject()
                            .field(FAILED_INDICES_FIELD.getPreferredName(), getFailedIndicesCount())
                            .field(FAILURES_FIELD.getPreferredName())
                            .startArray()
                    ),
                    failures.iterator(),
                    Iterators.single((b, p) -> b.endArray().endObject())
                )
                : Iterators.single((b, p) -> b.endObject().endObject())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesResponse that = (FieldCapabilitiesResponse) o;
        return Arrays.equals(indices, that.indices)
            && Objects.equals(resolvedLocally, that.resolvedLocally)
            && Objects.equals(resolvedRemotely, that.resolvedRemotely)
            && Objects.equals(fields, that.fields)
            && Objects.equals(indexResponses, that.indexResponses)
            && Objects.equals(failures, that.failures)
            && Objects.equals(minTransportVersion, that.minTransportVersion);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(resolvedLocally, resolvedRemotely, fields, indexResponses, failures, minTransportVersion);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return indexResponses.isEmpty() ? Strings.toString(this) : "FieldCapabilitiesResponse{unmerged}";
    }

    public static class Builder {
        private String[] indices = Strings.EMPTY_ARRAY;
        private ResolvedIndexExpressions resolvedLocally;
        private Map<String, ResolvedIndexExpressions> resolvedRemotely = Collections.emptyMap();
        private Map<String, Map<String, FieldCapabilities>> fields = Collections.emptyMap();
        private List<FieldCapabilitiesIndexResponse> indexResponses = Collections.emptyList();
        private List<FieldCapabilitiesFailure> failures = Collections.emptyList();
        private TransportVersion minTransportVersion = null;

        private Builder() {}

        public Builder withIndices(String[] indices) {
            this.indices = indices;
            return this;
        }

        public Builder withResolved(ResolvedIndexExpressions resolvedLocally, Map<String, ResolvedIndexExpressions> resolvedRemotely) {
            this.resolvedLocally = resolvedLocally;
            this.resolvedRemotely = resolvedRemotely;
            return this;
        }

        public Builder withResolvedRemotelyBuilder(Map<String, ResolvedIndexExpressions.Builder> resolvedRemotelyBuilder) {
            this.resolvedRemotely = resolvedRemotelyBuilder.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
            return this;
        }

        public Builder withResolvedRemotely(Map<String, ResolvedIndexExpressions> resolvedRemotely) {
            this.resolvedRemotely = resolvedRemotely;
            return this;
        }

        public Builder withResolvedLocally(ResolvedIndexExpressions resolvedLocally) {
            this.resolvedLocally = resolvedLocally;
            return this;
        }

        public Builder withFields(Map<String, Map<String, FieldCapabilities>> fields) {
            this.fields = fields;
            return this;
        }

        public Builder withIndexResponses(List<FieldCapabilitiesIndexResponse> indexResponses) {
            this.indexResponses = indexResponses;
            return this;
        }

        public Builder withFailures(List<FieldCapabilitiesFailure> failures) {
            this.failures = failures;
            return this;
        }

        public Builder withMinTransportVersion(TransportVersion minTransportVersion) {
            this.minTransportVersion = minTransportVersion;
            return this;
        }

        public FieldCapabilitiesResponse build() {
            return new FieldCapabilitiesResponse(
                indices,
                resolvedLocally,
                resolvedRemotely,
                fields,
                indexResponses,
                failures,
                minTransportVersion
            );
        }
    }
}
