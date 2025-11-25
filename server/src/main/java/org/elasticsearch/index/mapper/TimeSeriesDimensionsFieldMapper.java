/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

public class TimeSeriesDimensionsFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_timeseries";

    public static final String CONTENT_TYPE = "_timeseries";

    public static class Defaults {
        public static final String NAME = TimeSeriesDimensionsFieldMapper.NAME;

        public static final FieldType FIELD_TYPE;

        static {
            FieldType ft = new FieldType();
            ft.setIndexOptions(IndexOptions.NONE);
            ft.setStored(false);
            ft.setOmitNorms(true);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        public Builder() {
            super(Defaults.NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[0];
        }

        @Override
        public TimeSeriesDimensionsFieldMapper build() {
            return new TimeSeriesDimensionsFieldMapper();
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> new TimeSeriesDimensionsFieldMapper(), c -> new Builder());

    static final class TimeSeriesFieldType extends MappedFieldType {

        private TimeSeriesFieldType() {
            super(NAME, IndexType.NONE, false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new IllegalArgumentException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new QueryShardException(context, "The _timeseries field is not searchable");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(context, "The _timeseries field is not searchable");
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new TimeSeriesDimensionsFieldBlockLoader(blContext.dimensionFields());
        }
    }

    private TimeSeriesDimensionsFieldMapper() {
        super(new TimeSeriesFieldType());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
