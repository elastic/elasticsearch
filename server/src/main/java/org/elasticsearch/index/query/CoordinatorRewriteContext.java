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
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version in the coordinator.
 * Instances of this object rely on information stored in the {@code IndexMetadata} for certain indices.
 * Right now this context object is able to rewrite range queries that include a known timestamp field
 * (i.e. the timestamp field for DataStreams) into a MatchNoneQueryBuilder and skip the shards that
 * don't hold queried data. See IndexMetadata#getTimestampRange() for more details
 */
public class CoordinatorRewriteContext extends QueryRewriteContext {
    private final DateFieldRange atTimestampInfo; // Refers to '@timestamp' field
    // Refers to 'event.ingested' field
    private DateFieldRange eventIngestedInfo;  // TODO: make final

    /// MP TODO: do we need to add in fieldName to this record?
    public record DateFieldRange(DateFieldMapper.DateFieldType fieldType, IndexLongFieldRange fieldRange) {}

    public CoordinatorRewriteContext(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        DateFieldRange atTimestampRange  // MP TODO: add eventIngestedRange
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
        this.atTimestampInfo = atTimestampRange;
    }

    long getMinTimestamp(String fieldName) {
        /// MP TODO: are there static final entries for these field names somewhere?
        if (fieldName.equals("@timestamp")) {
            return atTimestampInfo.fieldRange().getMin();
        } else if (fieldName.equals("event.ingested")) {
            return eventIngestedInfo.fieldRange.getMin();
        } else {
            throw new IllegalArgumentException(
                "Only event.ingested or @timestamp are supported for min/max coordinator rewrites, but got: " + fieldName
            );
        }
    }

    long getMaxTimestamp(String fieldName) {
        /// MP TODO: are there static final entries for these field names somewhere?
        if (fieldName.equals("@timestamp")) {
            return atTimestampInfo.fieldRange().getMax();
        } else if (fieldName.equals("event.ingested")) {
            return eventIngestedInfo.fieldRange.getMax();
        } else {
            throw new IllegalArgumentException(
                "Only event.ingested or @timestamp are supported for min/max coordinator rewrites, but got: " + fieldName
            );
        }
    }

    boolean hasTimestampData(String fieldName) {
        if (fieldName.equals("@timestamp")) {
            return atTimestampInfo.fieldRange().isComplete() && atTimestampInfo.fieldRange() != IndexLongFieldRange.EMPTY;
        } else if (fieldName.equals("event.ingested")) {
            return eventIngestedInfo.fieldRange().isComplete() && eventIngestedInfo.fieldRange() != IndexLongFieldRange.EMPTY;
        } else {
            throw new IllegalArgumentException(
                "Only event.ingested or @timestamp are supported for min/max coordinator rewrites, but got: " + fieldName
            );
        }
    }

    @Nullable  /// MP TODO: why is this nullable? can we remove this?
    public MappedFieldType getFieldType(String fieldName) {
        if (fieldName.equals("@timestamp")) {
            return atTimestampInfo.fieldType();
        } else if (fieldName.equals("event.ingested")) {
            return eventIngestedInfo.fieldType();
        } else {
            return null; // MP TODO: do we want to throw exception here too?
            // throw new IllegalArgumentException("Only event.ingested or @timestamp are supported for min/max coordinator rewrites, but
            // got: "
            // + fieldName);
        }
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }
}
