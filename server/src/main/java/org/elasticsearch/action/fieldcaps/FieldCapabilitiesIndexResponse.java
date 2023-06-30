/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class FieldCapabilitiesIndexResponse implements Writeable {
    private static final TransportVersion MAPPING_HASH_VERSION = TransportVersion.V_8_2_0;

    private final String indexName;
    @Nullable
    private final String indexMappingHash;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient TransportVersion originVersion;

    FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch
    ) {
        this.indexName = indexName;
        this.indexMappingHash = indexMappingHash;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = TransportVersion.current();
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.responseMap = in.readMap(IndexFieldCapabilities::new);
        this.canMatch = in.readBoolean();
        this.originVersion = in.getTransportVersion();
        if (in.getTransportVersion().onOrAfter(MAPPING_HASH_VERSION)) {
            this.indexMappingHash = in.readOptionalString();
        } else {
            this.indexMappingHash = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(responseMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        out.writeBoolean(canMatch);
        if (out.getTransportVersion().onOrAfter(MAPPING_HASH_VERSION)) {
            out.writeOptionalString(indexMappingHash);
        }
    }

    private record GroupByMappingHash(List<String> indices, String indexMappingHash, Map<String, IndexFieldCapabilities> responseMap)
        implements
            Writeable {
        GroupByMappingHash(StreamInput in) throws IOException {
            this(in.readStringList(), in.readString(), in.readMap(IndexFieldCapabilities::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indices);
            out.writeString(indexMappingHash);
            out.writeMap(responseMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        }

        List<FieldCapabilitiesIndexResponse> getResponses() {
            return indices.stream().map(index -> new FieldCapabilitiesIndexResponse(index, indexMappingHash, responseMap, true)).toList();
        }
    }

    static List<FieldCapabilitiesIndexResponse> readList(StreamInput input) throws IOException {
        if (input.getTransportVersion().before(MAPPING_HASH_VERSION)) {
            return input.readList(FieldCapabilitiesIndexResponse::new);
        }
        final List<FieldCapabilitiesIndexResponse> ungroupedList = input.readList(FieldCapabilitiesIndexResponse::new);
        final List<GroupByMappingHash> groups = input.readList(GroupByMappingHash::new);
        return Stream.concat(ungroupedList.stream(), groups.stream().flatMap(g -> g.getResponses().stream())).toList();
    }

    static void writeList(StreamOutput output, List<FieldCapabilitiesIndexResponse> responses) throws IOException {
        if (output.getTransportVersion().before(MAPPING_HASH_VERSION)) {
            output.writeCollection(responses);
            return;
        }
        final Predicate<FieldCapabilitiesIndexResponse> canGroup = r -> r.canMatch && r.indexMappingHash != null;
        final List<FieldCapabilitiesIndexResponse> ungroupedResponses = responses.stream().filter(r -> canGroup.test(r) == false).toList();
        final List<GroupByMappingHash> groupedResponses = responses.stream()
            .filter(canGroup)
            .collect(Collectors.groupingBy(r -> r.indexMappingHash))
            .values()
            .stream()
            .map(rs -> {
                final String indexMappingHash = rs.get(0).indexMappingHash;
                final Map<String, IndexFieldCapabilities> responseMap = rs.get(0).responseMap;
                final List<String> indices = rs.stream().map(r -> r.indexName).toList();
                return new GroupByMappingHash(indices, indexMappingHash, responseMap);
            })
            .toList();
        output.writeList(ungroupedResponses);
        output.writeList(groupedResponses);
    }

    /**
     * Get the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the index mapping hash associated with this index if exists
     */
    @Nullable
    public String getIndexMappingHash() {
        return indexMappingHash;
    }

    public boolean canMatch() {
        return canMatch;
    }

    /**
     * Get the field capabilities map
     */
    public Map<String, IndexFieldCapabilities> get() {
        return responseMap;
    }

    TransportVersion getOriginVersion() {
        return originVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesIndexResponse that = (FieldCapabilitiesIndexResponse) o;
        return canMatch == that.canMatch
            && Objects.equals(indexName, that.indexName)
            && Objects.equals(indexMappingHash, that.indexMappingHash)
            && Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexMappingHash, responseMap, canMatch);
    }
}
