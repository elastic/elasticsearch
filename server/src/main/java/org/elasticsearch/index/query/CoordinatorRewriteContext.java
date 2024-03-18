/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version in the coordinator.
 * Instances of this object rely on information stored in the {@code IndexMetadata} for certain indices.
 * Right now this context object is able to rewrite range queries that include a known timestamp field
 * (i.e. the timestamp field for DataStreams) into a MatchNoneQueryBuilder and skip the shards that
 * don't hold queried data. See IndexMetadata#getTimestampRange() for more details
 */
public class CoordinatorRewriteContext extends QueryRewriteContext {
    private final IndexLongFieldRange indexLongFieldRange;
    private final Map<String, DateFieldMapper.DateFieldType> timestampFieldTypes;

    public CoordinatorRewriteContext(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        IndexLongFieldRange indexLongFieldRange,
        Map<String, DateFieldMapper.DateFieldType> timestampFieldTypes
    ) {
        super(
            parserConfig,
            client,
            nowInMillis,
            null,
            MappingLookup.EMPTY,
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        this.indexLongFieldRange = indexLongFieldRange;
        this.timestampFieldTypes = timestampFieldTypes;
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
        return timestampFieldTypes.get(fieldName);
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }
}
