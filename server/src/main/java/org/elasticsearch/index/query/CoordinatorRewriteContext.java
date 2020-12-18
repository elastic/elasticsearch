/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.IndexLongFieldRange;

import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version in the coordinator.
 * Instances of this object rely on information stored in the {@code IndexMetadata} for certain indices.
 * Right now this context object is able to rewrite range queries that include a known timestamp field
 * (i.e. the timestamp field for DataStreams) into a MatchNoneQueryBuilder and skip the shards that
 * don't hold queried data. See IndexMetadata#getTimestampMillisRange() for more details
 */
public class CoordinatorRewriteContext extends QueryRewriteContext {
    private final Index index;
    private IndexLongFieldRange indexLongFieldRange;
    private final DateFieldMapper.DateFieldType timestampFieldType;

    public CoordinatorRewriteContext(NamedXContentRegistry xContentRegistry,
                                     NamedWriteableRegistry writeableRegistry,
                                     Client client,
                                     LongSupplier nowInMillis,
                                     Index index,
                                     IndexLongFieldRange indexLongFieldRange,
                                     DateFieldMapper.DateFieldType timestampFieldType) {
        super(xContentRegistry, writeableRegistry, client, nowInMillis);
        this.index = index;
        this.indexLongFieldRange = indexLongFieldRange;
        this.timestampFieldType = timestampFieldType;
    }

    long getMinTimestamp() {
        return indexLongFieldRange.getMin();
    }

    long getMaxTimestamp() {
        return indexLongFieldRange.getMax();
    }

    boolean hasTimestampData() {
        return indexLongFieldRange.isComplete() && indexLongFieldRange != IndexLongFieldRange.EMPTY;
    }

    @Nullable
    public MappedFieldType getFieldType(String fieldName) {
        if (fieldName.equals(timestampFieldType.name()) == false) {
            return null;
        }

        return timestampFieldType;
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }
}
