/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

public class FieldCapabilitiesIndexRequest extends ActionRequest implements IndicesRequest {

    public static final IndicesOptions INDICES_OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private final String[] fields;
    private final OriginalIndices originalIndices;
    private final QueryBuilder indexFilter;
    private final long nowInMillis;
    private final Map<String, Object> runtimeFields;
    private final ShardId shardId;

    // For serialization
    FieldCapabilitiesIndexRequest(StreamInput in) throws IOException {
        super(in);
        shardId = in.readOptionalWriteable(ShardId::new);
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readOptionalString(); // index
        }
        fields = in.readStringArray();
        originalIndices = OriginalIndices.readOriginalIndices(in);
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nowInMillis =  in.readLong();
        runtimeFields = in.readMap();
    }

    FieldCapabilitiesIndexRequest(String[] fields,
                                  ShardId shardId,
                                  OriginalIndices originalIndices,
                                  QueryBuilder indexFilter,
                                  long nowInMillis,
                                  Map<String, Object> runtimeFields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        this.shardId = shardId;
        this.fields = fields;
        this.originalIndices = originalIndices;
        this.indexFilter = indexFilter;
        this.nowInMillis = nowInMillis;
        this.runtimeFields = runtimeFields;
    }

    public String[] fields() {
        return fields;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    public String index() {
        return shardId.getIndexName();
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    public Map<String, Object> runtimeFields() {
        return runtimeFields;
    }

    public ShardId shardId() {
        return shardId;
    }

    public long nowInMillis() {
        return nowInMillis;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(shardId);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeOptionalString(shardId.getIndexName());
        }
        out.writeStringArray(fields);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeLong(nowInMillis);
        out.writeMap(runtimeFields);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
