/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetFieldMappingsIndexRequest extends SingleShardRequest<GetFieldMappingsIndexRequest> {

    private final boolean includeDefaults;
    private final String[] fields;

    private final OriginalIndices originalIndices;

    GetFieldMappingsIndexRequest(StreamInput in) throws IOException {
        super(in);
        fields = in.readStringArray();
        includeDefaults = in.readBoolean();
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    GetFieldMappingsIndexRequest(GetFieldMappingsRequest other, String index) {
        this.includeDefaults = other.includeDefaults();
        this.fields = other.fields();
        assert index != null;
        this.index(index);
        this.originalIndices = new OriginalIndices(other);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String[] fields() {
        return fields;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
        out.writeBoolean(includeDefaults);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

}
