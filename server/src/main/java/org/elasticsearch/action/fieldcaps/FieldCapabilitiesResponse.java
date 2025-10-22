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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Response for {@link FieldCapabilitiesRequest} requests.
 */
public class FieldCapabilitiesResponse extends ActionResponse implements ChunkedToXContentObject {

    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField FAILED_INDICES_FIELD = new ParseField("failed_indices");
    public static final ParseField FAILURES_FIELD = new ParseField("failures");

    private static final TransportVersion RESOLVED_FIELDS_CAPS = TransportVersion.fromName("resolved_fields_caps");

    private final String[] indices;
    private final ResolvedIndexExpressions resolvedLocally;
    private final transient Map<String, ResolvedIndexExpressions> resolvedRemotely;
    private final Map<String, Map<String, FieldCapabilities>> fields;
    private final List<FieldCapabilitiesFailure> failures;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;
    private final TransportVersion minTransportVersion;

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
        this.resolvedLocally = in.getTransportVersion().supports(RESOLVED_FIELDS_CAPS)
            ? in.readOptionalWriteable(ResolvedIndexExpressions::new)
            : null;
        this.resolvedRemotely = Collections.emptyMap();
        this.minTransportVersion = in.getTransportVersion().supports(RESOLVED_FIELDS_CAPS)
            ? in.readOptional(TransportVersion::readVersion)
            : null;
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
     * Locally resolved index expressions
     */
    public Map<String, ResolvedIndexExpressions> getResolvedRemotely() {
        return resolvedRemotely;
    }

    /**
     *
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
        if (out.getTransportVersion().supports(RESOLVED_FIELDS_CAPS)) {
            out.writeOptionalWriteable(resolvedLocally);
        }
        if (out.getTransportVersion().supports(RESOLVED_FIELDS_CAPS)) {
            out.writeOptional((Writer<TransportVersion>) (o, v) -> TransportVersion.writeVersion(v, o), minTransportVersion);
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
        return Objects.hash(resolvedLocally, resolvedRemotely, fields, indexResponses, failures, minTransportVersion) * 31 + Arrays
            .hashCode(indices);
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
        private TransportVersion minTransportVersion;

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

        public Builder withResolvedLocally(ResolvedIndexExpressions resolvedLocally) {
            this.resolvedLocally = resolvedLocally;
            return this;
        }

        public Builder withFields(Map<String, Map<String, FieldCapabilities>> fields) {
            this.fields = fields;
            return this;
        }

        public Builder withIndexResponses(Collection<FieldCapabilitiesIndexResponse> indexResponses) {
            this.indexResponses = new ArrayList<>(indexResponses);
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
