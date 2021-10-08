/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class FieldCapabilitiesNodeResponse extends ActionResponse implements Writeable {
    private final List<String> indices;
    private final Map<String, Map<String, FieldCapabilities.Builder>> responseMap;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;
    private final List<FieldCapabilitiesFailure> indexFailures;
    private final Set<ShardId> unmatchedShardIds;

    FieldCapabilitiesNodeResponse(List<FieldCapabilitiesIndexResponse> indexResponses,
                                  List<FieldCapabilitiesFailure> indexFailures,
                                  Set<ShardId> unmatchedShardIds) {
        this.indices = Collections.emptyList();
        this.responseMap = Collections.emptyMap();
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.indexFailures = Objects.requireNonNull(indexFailures);
        this.unmatchedShardIds = unmatchedShardIds;
    }

    FieldCapabilitiesNodeResponse(List<String> indices,
                                  Map<String, Map<String, FieldCapabilities.Builder>> responseMap,
                                  List<FieldCapabilitiesFailure> indexFailures,
                                  Set<ShardId> unmatchedShardIds) {
        this.indices = indices;
        this.indexResponses = Collections.emptyList();
        this.responseMap = responseMap;
        this.indexFailures = Objects.requireNonNull(indexFailures);
        this.unmatchedShardIds = unmatchedShardIds;
    }


    FieldCapabilitiesNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringList();
        this.responseMap = in.readMap(StreamInput::readString, FieldCapabilities::readBuilderField);
        this.indexResponses = in.readList(FieldCapabilitiesIndexResponse::new);
        this.indexFailures = in.readList(FieldCapabilitiesFailure::new);
        this.unmatchedShardIds = in.readSet(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(indices);
        out.writeMap(responseMap, StreamOutput::writeString, FieldCapabilities::writeBuilderField);
        out.writeList(indexResponses);
        out.writeList(indexFailures);
        out.writeCollection(unmatchedShardIds);
    }

    List<String> getIndices() {
        return indices;
    }

    Map<String, Map<String, FieldCapabilities.Builder>> getResponseMap() {
        return responseMap;
    }

    List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    List<FieldCapabilitiesFailure> getIndexFailures() {
        return indexFailures;
    }

    Set<ShardId> getUnmatchedShardIds() {
        return unmatchedShardIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesNodeResponse that = (FieldCapabilitiesNodeResponse) o;
        return Objects.equals(indexResponses, that.indexResponses) && Objects.equals(indexFailures, that.indexFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexResponses, indexFailures);
    }
}
