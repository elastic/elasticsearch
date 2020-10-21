/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import java.util.Objects;

public class FieldCapabilitiesIndexRequest extends ActionRequest implements IndicesRequest {

    public static final IndicesOptions INDICES_OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private final String index;
    private final String[] fields;
    private final OriginalIndices originalIndices;
    private final QueryBuilder indexFilter;
    private final long nowInMillis;

    private ShardId shardId;

    // For serialization
    FieldCapabilitiesIndexRequest(StreamInput in) throws IOException {
        super(in);
        shardId = in.readOptionalWriteable(ShardId::new);
        index = in.readOptionalString();
        fields = in.readStringArray();
        originalIndices = OriginalIndices.readOriginalIndices(in);
        indexFilter = in.getVersion().onOrAfter(Version.V_7_9_0) ? in.readOptionalNamedWriteable(QueryBuilder.class) : null;
        nowInMillis =  in.getVersion().onOrAfter(Version.V_7_9_0) ? in.readLong() : 0L;
    }

    FieldCapabilitiesIndexRequest(String[] fields,
                                  String index,
                                  OriginalIndices originalIndices,
                                  QueryBuilder indexFilter,
                                  long nowInMillis) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        this.index = Objects.requireNonNull(index);
        this.fields = fields;
        this.originalIndices = originalIndices;
        this.indexFilter = indexFilter;
        this.nowInMillis = nowInMillis;
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
        return index;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    public ShardId shardId() {
        return shardId;
    }

    public long nowInMillis() {
        return nowInMillis;
    }

    FieldCapabilitiesIndexRequest shardId(ShardId shardId) {
        this.shardId = shardId;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(shardId);
        out.writeOptionalString(index);
        out.writeStringArray(fields);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeOptionalNamedWriteable(indexFilter);
            out.writeLong(nowInMillis);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
