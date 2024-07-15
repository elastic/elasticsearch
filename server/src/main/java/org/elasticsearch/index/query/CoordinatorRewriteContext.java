/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.Collections;
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version in the coordinator.
 * Instances of this object rely on information stored in the {@code IndexMetadata} for certain indices.
 * Right now this context object is able to rewrite range queries that include a known timestamp field
 * (i.e. the timestamp field for DataStreams or the 'event.ingested' field in ECS) into a MatchNoneQueryBuilder
 * and skip the shards that don't hold queried data. See IndexMetadata for more details.
 */
public class CoordinatorRewriteContext extends QueryRewriteContext {
    private final DateFieldRangeInfo dateFieldRangeInfo;

    /**
     * Context for coordinator search rewrites based on time ranges for the @timestamp field and/or 'event.ingested' field
     * @param parserConfig
     * @param client
     * @param nowInMillis
     * @param dateFieldRangeInfo range and field type info for @timestamp and 'event.ingested'
     */
    public CoordinatorRewriteContext(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        DateFieldRangeInfo dateFieldRangeInfo
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
        this.dateFieldRangeInfo = dateFieldRangeInfo;
    }

    /**
     * Get min timestamp for either '@timestamp' or 'event.ingested' fields. Any other field
     * passed in will cause an {@link IllegalArgumentException} to be thrown, as these are the only
     * two fields supported for coordinator rewrites (based on time range).
     * @param fieldName Must be DataStream.TIMESTAMP_FIELD_NAME or IndexMetadata.EVENT_INGESTED_FIELD_NAME
     * @return min timestamp for the field from IndexMetadata in cluster state.
     */
    long getMinTimestamp(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getTimestampRange().getMin();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getEventIngestedRange().getMin();
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "Only [%s] or [%s] fields are supported for min timestamp coordinator rewrites, but got: [%s]",
                    DataStream.TIMESTAMP_FIELD_NAME,
                    IndexMetadata.EVENT_INGESTED_FIELD_NAME,
                    fieldName
                )
            );
        }
    }

    /**
     * Get max timestamp for either '@timestamp' or 'event.ingested' fields. Any other field
     * passed in will cause an {@link IllegalArgumentException} to be thrown, as these are the only
     * two fields supported for coordinator rewrites (based on time range).
     * @param fieldName Must be DataStream.TIMESTAMP_FIELD_NAME or IndexMetadata.EVENT_INGESTED_FIELD_NAME
     * @return max timestamp for the field from IndexMetadata in cluster state.
     */
    long getMaxTimestamp(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getTimestampRange().getMax();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getEventIngestedRange().getMax();
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "Only [%s] or [%s] fields are supported for max timestamp coordinator rewrites, but got: [%s]",
                    DataStream.TIMESTAMP_FIELD_NAME,
                    IndexMetadata.EVENT_INGESTED_FIELD_NAME,
                    fieldName
                )
            );
        }
    }

    /**
     * Determine whether either '@timestamp' or 'event.ingested' fields has useful timestamp ranges
     * stored in cluster state for this context.
     * Any other fieldname will cause an {@link IllegalArgumentException} to be thrown, as these are the only
     * two fields supported for coordinator rewrites (based on time range).
     * @param fieldName Must be DataStream.TIMESTAMP_FIELD_NAME or IndexMetadata.EVENT_INGESTED_FIELD_NAME
     * @return min timestamp for the field from IndexMetadata in cluster state.
     */
    boolean hasTimestampData(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getTimestampRange().isComplete()
                && dateFieldRangeInfo.getTimestampRange() != IndexLongFieldRange.EMPTY
                && dateFieldRangeInfo.getTimestampRange() != IndexLongFieldRange.UNKNOWN;
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getEventIngestedRange().isComplete()
                && dateFieldRangeInfo.getEventIngestedRange() != IndexLongFieldRange.EMPTY
                && dateFieldRangeInfo.getEventIngestedRange() != IndexLongFieldRange.UNKNOWN;
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "Only [%s] or [%s] fields are supported for min/max timestamp coordinator rewrites, but got: [%s]",
                    DataStream.TIMESTAMP_FIELD_NAME,
                    IndexMetadata.EVENT_INGESTED_FIELD_NAME,
                    fieldName
                )
            );
        }
    }

    /**
     * @param fieldName Get MappedFieldType for either '@timestamp' or 'event.ingested' fields.
     * @return min timestamp for the field from IndexMetadata in cluster state or null if fieldName was not
     *         DataStream.TIMESTAMP_FIELD_NAME or IndexMetadata.EVENT_INGESTED_FIELD_NAME.
     */
    @Nullable
    public MappedFieldType getFieldType(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getTimestampFieldType();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.getEventIngestedFieldType();
        } else {
            return null;
        }
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }
}
