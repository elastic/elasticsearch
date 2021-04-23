/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

/**
 * This meta field only exists because rank feature fields index everything into a
 * common _feature field and Elasticsearch has a custom codec that complains
 * when fields exist in the index and not in mappings.
 */
public class RankFeatureMetaFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_feature";

    public static final String CONTENT_TYPE = "_feature";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new RankFeatureMetaFieldMapper());

    public static final class RankFeatureMetaFieldType extends MappedFieldType {

        public static final RankFeatureMetaFieldType INSTANCE = new RankFeatureMetaFieldType();

        private RankFeatureMetaFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + typeName() + "].");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on [_feature]");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new UnsupportedOperationException("The [_feature] field may not be queried directly");
        }
    }

    private RankFeatureMetaFieldMapper() {
        super(RankFeatureMetaFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
