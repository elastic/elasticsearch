/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

public class LegacyTypeFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_type";

    public static final String CONTENT_TYPE = "_type";

    private static final LegacyTypeFieldMapper INSTANCE = new LegacyTypeFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    protected LegacyTypeFieldMapper() {
        super(new LegacyTypeFieldType(), Lucene.KEYWORD_ANALYZER);
    }

    public static class Defaults {

        public static final FieldType FIELD_TYPE = new FieldType();
        public static final FieldType NESTED_FIELD_TYPE;

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE = new FieldType();
            NESTED_FIELD_TYPE.setTokenized(false);
            NESTED_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.setOmitNorms(true);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    static final class LegacyTypeFieldType extends TermBasedFieldType {

        LegacyTypeFieldType() {
            super(NAME, false, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean allowDocValueBasedQueries() {
            return true;
        }

        @Override
        public boolean isSearchable() {
            // The _type field is always searchable.
            return true;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
