/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

/**
 * Mapper for the {@code _ts_metric_names_hash} field.
 *
 * The field contains a hash of the field names that are a time_series_metric.
 * It's stored to be retrieved when reconstructing the _id field in search queries.
 */
public class TimeSeriesMetricNamesHashFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_ts_metric_names_hash";

    public static final TimeSeriesMetricNamesHashFieldMapper INSTANCE = new TimeSeriesMetricNamesHashFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().timeSeriesMetricNamesHashFieldMapper());
    static final NodeFeature TS_METRIC_NAMES_HASH = new NodeFeature("tsdb.ts_metric_names_hash", true);

    private TimeSeriesMetricNamesHashFieldMapper() {
        super(TimeSeriesMetricNamesHashFieldType.INSTANCE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (context.indexSettings().getMode() == IndexMode.TIME_SERIES
            && context.indexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_METRIC_NAMES_HASH_IN_ID)) {
            final RoutingPathFields routingPathFields = (RoutingPathFields) context.getRoutingFields();
            byte[] bytes = new byte[8];
            ByteUtils.writeLongLE(routingPathFields.buildMetricNamesHash(), bytes, 0);
            // Uses sorted doc values instead of numeric so that we can leverage the run-length encoding applied to ordinals
            context.rootDoc().add(new SortedDocValuesField(NAME, new BytesRef(bytes)));
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    static final class TimeSeriesMetricNamesHashFieldType extends MappedFieldType {

        private static final TimeSeriesMetricNamesHashFieldType INSTANCE = new TimeSeriesMetricNamesHashFieldType();

        private TimeSeriesMetricNamesHashFieldType() {
            super(NAME, false, false, true, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("[" + NAME + "] is not searchable");
        }
    }
}
