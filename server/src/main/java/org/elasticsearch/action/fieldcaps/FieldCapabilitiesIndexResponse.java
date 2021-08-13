/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Response for shard level operation in {@link TransportFieldCapabilitiesAction}.
 */
public class FieldCapabilitiesIndexResponse extends ActionResponse implements Writeable {
    private final String indexName;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient Version originVersion;

    FieldCapabilitiesIndexResponse(String indexName, Map<String, IndexFieldCapabilities> responseMap, boolean canMatch) {
        this.indexName = indexName;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = Version.CURRENT;
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.responseMap = in.readMap(StreamInput::readString, IndexFieldCapabilities::new);
        this.canMatch = in.readBoolean();
        this.originVersion = in.getVersion();
    }

    /**
     * Get the index name
     */
    public String getIndexName() {
        return indexName;
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

    /**
     *
     * Get the field capabilities for the provided {@code field}
     */
    public IndexFieldCapabilities getField(String field) {
        return responseMap.get(field);
    }

    Version getOriginVersion() {
        return originVersion;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(responseMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        out.writeBoolean(canMatch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesIndexResponse that = (FieldCapabilitiesIndexResponse) o;
        return canMatch == that.canMatch &&
            Objects.equals(indexName, that.indexName) &&
            Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, responseMap, canMatch);
    }
}
