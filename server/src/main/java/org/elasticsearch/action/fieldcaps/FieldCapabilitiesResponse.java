/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Response for {@link FieldCapabilitiesRequest} requests.
 */
public class FieldCapabilitiesResponse extends ActionResponse implements ChunkedToXContentObject {
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField FAILED_INDICES_FIELD = new ParseField("failed_indices");
    public static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final String[] indices;
    private final Map<String, Map<String, FieldCapabilities>> responseMap;
    private final List<FieldCapabilitiesFailure> failures;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;

    public FieldCapabilitiesResponse(
        String[] indices,
        Map<String, Map<String, FieldCapabilities>> responseMap,
        List<FieldCapabilitiesFailure> failures
    ) {
        this(indices, responseMap, Collections.emptyList(), failures);
    }

    public FieldCapabilitiesResponse(String[] indices, Map<String, Map<String, FieldCapabilities>> responseMap) {
        this(indices, responseMap, Collections.emptyList(), Collections.emptyList());
    }

    public FieldCapabilitiesResponse(List<FieldCapabilitiesIndexResponse> indexResponses, List<FieldCapabilitiesFailure> failures) {
        this(Strings.EMPTY_ARRAY, Collections.emptyMap(), indexResponses, failures);
    }

    private FieldCapabilitiesResponse(
        String[] indices,
        Map<String, Map<String, FieldCapabilities>> responseMap,
        List<FieldCapabilitiesIndexResponse> indexResponses,
        List<FieldCapabilitiesFailure> failures
    ) {
        this.responseMap = Objects.requireNonNull(responseMap);
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.indices = indices;
        this.failures = failures;
    }

    public FieldCapabilitiesResponse(StreamInput in) throws IOException {
        indices = in.readStringArray();
        this.responseMap = in.readMap(FieldCapabilitiesResponse::readField);
        this.indexResponses = FieldCapabilitiesIndexResponse.readList(in);
        this.failures = in.readCollectionAsList(FieldCapabilitiesFailure::new);
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
        return responseMap;
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
     *
     * Get the field capabilities per type for the provided {@code field}.
     */
    public Map<String, FieldCapabilities> getField(String field) {
        return responseMap.get(field);
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
        out.writeMap(responseMap, FieldCapabilitiesResponse::writeField);
        FieldCapabilitiesIndexResponse.writeList(out, indexResponses);
        out.writeCollection(failures);
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
            Iterators.map(responseMap.entrySet().iterator(), r -> (b, p) -> b.xContentValuesMap(r.getKey(), r.getValue())),
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
            && Objects.equals(responseMap, that.responseMap)
            && Objects.equals(indexResponses, that.indexResponses)
            && Objects.equals(failures, that.failures);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(responseMap, indexResponses, failures);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        if (indexResponses.size() > 0) {
            return "FieldCapabilitiesResponse{unmerged}";
        }
        return Strings.toString(this);
    }
}
