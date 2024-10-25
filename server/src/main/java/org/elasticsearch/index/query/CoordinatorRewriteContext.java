/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version in the coordinator.
 * Instances of this object rely on information stored in the {@code IndexMetadata} for certain indices.
 * Right now this context object is able to rewrite range queries that include a known timestamp field
 * (i.e. the timestamp field for DataStreams or the 'event.ingested' field in ECS) into a MatchNoneQueryBuilder
 * and skip the shards that don't hold queried data. See IndexMetadata for more details.
 */
public class CoordinatorRewriteContext extends QueryRewriteContext {

    public static final String TIER_FIELD_NAME = "_tier";

    private static final ConstantFieldType TIER_FIELD_TYPE = new ConstantFieldType(TIER_FIELD_NAME, Map.of()) {
        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("fetching field values is not supported on the coordinator node");
        }

        @Override
        public String typeName() {
            return TIER_FIELD_NAME;
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, QueryRewriteContext context) {
            if (caseInsensitive) {
                pattern = Strings.toLowercaseAscii(pattern);
            }

            String tierPreference = context.getTierPreference();
            if (tierPreference == null) {
                return false;
            }
            return Regex.simpleMatch(pattern, tierPreference);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException("field exists query is not supported on the coordinator node");
        }
    };

    private final DateFieldRangeInfo dateFieldRangeInfo;
    private final String tier;

    /**
     * Context for coordinator search rewrites based on time ranges for the @timestamp field and/or 'event.ingested' field
     *
     * @param parserConfig
     * @param client
     * @param nowInMillis
     * @param dateFieldRangeInfo range and field type info for @timestamp and 'event.ingested'
     * @param tier               the configured data tier (via the _tier_preference setting) for the index
     */
    public CoordinatorRewriteContext(
        XContentParserConfiguration parserConfig,
        Client client,
        LongSupplier nowInMillis,
        DateFieldRangeInfo dateFieldRangeInfo,
        String tier
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
            null,
            null
        );
        this.dateFieldRangeInfo = dateFieldRangeInfo;
        this.tier = tier;
    }

    /**
     * @param fieldName Must be one of DataStream.TIMESTAMP_FIELD_FIELD, IndexMetadata.EVENT_INGESTED_FIELD_NAME, or
     *                  DataTierFiledMapper.NAME
     * @return MappedField with type for the field. Returns null if fieldName is not one of the allowed field names.
     */
    @Nullable
    public MappedFieldType getFieldType(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.timestampFieldType();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.eventIngestedFieldType();
        } else if (TIER_FIELD_NAME.equals(fieldName)) {
            return TIER_FIELD_TYPE;
        } else {
            return null;
        }
    }

    /**
     * @param fieldName Must be one of DataStream.TIMESTAMP_FIELD_FIELD or IndexMetadata.EVENT_INGESTED_FIELD_NAME
     * @return IndexLongFieldRange with min/max ranges for the field. Returns null if fieldName is not one of the allowed field names.
     */
    @Nullable
    public IndexLongFieldRange getFieldRange(String fieldName) {
        if (DataStream.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.timestampRange();
        } else if (IndexMetadata.EVENT_INGESTED_FIELD_NAME.equals(fieldName)) {
            return dateFieldRangeInfo.eventIngestedRange();
        } else {
            return null;
        }
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }

    @Override
    public String getTierPreference() {
        // dominant branch first (tier preference is configured)
        return tier.isEmpty() == false ? tier : null;
    }

    /**
     * We're holding on to the index tier in the context as otherwise we'd need
     * to re-parse it from the index settings when evaluating the _tier field.
     */
    public String tier() {
        return tier;
    }
}
