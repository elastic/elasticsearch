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
 * (i.e. the timestamp field for DataStreams or the 'event.ingested' field in ECS) into a MatchNoneQueryBuilder
 * and skip the shards that don't hold queried data. See IndexMetadata for more details.
 */
public class CoordinatorRewriteContext extends QueryRewriteContext {
    private final DateFieldRange timestampInfo; // Refers to '@timestamp' field
    private final DateFieldRange eventIngestedInfo; // Refers to 'event.ingested' field

    /**
     * Date range record that collates a DateFieldType with an IndexLongFieldRange.
     * Used to hold ranges for the @timestamp and 'event.ingested' date fields, which are held in
     * cluster state.
     * @param fieldType DateFieldType for @timestamp or 'event.ingested'
     * @param fieldRange the range for the field type
     */
    public record DateFieldRange(DateFieldMapper.DateFieldType fieldType, IndexLongFieldRange fieldRange) {}

    /**
     * Context for coordinator search rewrites based on time ranges for the @timestamp field and/or 'event.ingested' field
     * @param parserConfig
     * @param client
     * @param nowInMillis
     * @param timestampRange range for @timestamp
     * @param eventIngestedRange range for 'event.ingested'
     */
    public CoordinatorRewriteContext(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        DateFieldRange timestampRange,
        DateFieldRange eventIngestedRange
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
        this.timestampInfo = timestampRange;
        this.eventIngestedInfo = eventIngestedRange;
        // TODO: do we need an assert here to ensure neither range arg is null? Or are they @Nullable?
    }

    long getMinTimestamp(String fieldName) {
        if (fieldName.equals(DataStream.TIMESTAMP_FIELD_NAME)) {
            return timestampInfo.fieldRange().getMin();
        } else if (fieldName.equals(IndexMetadata.EVENT_INGESTED_FIELD_NAME)) {
            return eventIngestedInfo.fieldRange().getMin(); // TODO: can this throw NPE
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

    long getMaxTimestamp(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return timestampInfo.fieldRange().getMax();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return eventIngestedInfo.fieldRange().getMax(); // TODO: can this throw NPE
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

    boolean hasTimestampData(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            // TODO: again possible NPE here?
            return timestampInfo.fieldRange().isComplete() && timestampInfo.fieldRange() != IndexLongFieldRange.EMPTY;
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            // TODO: again possible NPE here?
            return eventIngestedInfo.fieldRange().isComplete() && eventIngestedInfo.fieldRange() != IndexLongFieldRange.EMPTY;
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

    @Nullable
    public MappedFieldType getFieldType(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return timestampInfo.fieldType();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return eventIngestedInfo.fieldType();
        } else {
            return null;
        }
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }
}
