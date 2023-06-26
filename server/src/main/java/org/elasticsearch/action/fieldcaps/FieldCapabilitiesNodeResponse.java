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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class FieldCapabilitiesNodeResponse extends ActionResponse implements Writeable {
    private final List<FieldCapabilitiesIndexResponse> indexResponses;
    private final Map<ShardId, Exception> failures;
    private final Set<ShardId> unmatchedShardIds;

    FieldCapabilitiesNodeResponse(
        List<FieldCapabilitiesIndexResponse> indexResponses,
        Map<ShardId, Exception> failures,
        Set<ShardId> unmatchedShardIds
    ) {
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.failures = Objects.requireNonNull(failures);
        this.unmatchedShardIds = Objects.requireNonNull(unmatchedShardIds);
    }

    FieldCapabilitiesNodeResponse(StreamInput in) throws IOException {
        super(in);
        final IndexFieldCapabilities.Deduplicator fieldDeduplicator = new IndexFieldCapabilities.Deduplicator();
        this.indexResponses = in.readList(is -> new FieldCapabilitiesIndexResponse(is, fieldDeduplicator));
        this.failures = in.readMap(ShardId::new, StreamInput::readException);
        this.unmatchedShardIds = in.readSet(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(indexResponses);
        out.writeMap(failures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
        out.writeCollection(unmatchedShardIds);
    }

    public Map<ShardId, Exception> getFailures() {
        return failures;
    }

    public List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    public Set<ShardId> getUnmatchedShardIds() {
        return unmatchedShardIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesNodeResponse that = (FieldCapabilitiesNodeResponse) o;
        return Objects.equals(indexResponses, that.indexResponses)
            && Objects.equals(failures, that.failures)
            && unmatchedShardIds.equals(that.unmatchedShardIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexResponses, failures, unmatchedShardIds);
    }
}
