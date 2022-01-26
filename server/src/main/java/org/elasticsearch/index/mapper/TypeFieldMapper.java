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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;

public class TypeFieldMapper extends MetadataFieldMapper {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TypeFieldType.class);

    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using the _type field "
        + "in queries and aggregations is deprecated, prefer to use a field instead.";

    public static void emitTypesDeprecationWarning() {
        deprecationLogger.critical(DeprecationCategory.TYPES, "query_with_types", TYPES_DEPRECATION_MESSAGE);
    }

    public static final String NAME = "_type";

    public static final String CONTENT_TYPE = "_type";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new TypeFieldMapper());

    public static class Defaults {

        public static final FieldType NESTED_FIELD_TYPE = new FieldType();

        static {
            NESTED_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            NESTED_FIELD_TYPE.setTokenized(false);
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.setOmitNorms(true);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    public static final class TypeFieldType extends ConstantFieldType {

        private final String type;

        public TypeFieldType(String type) {
            super(NAME, Collections.emptyMap());
            this.type = type;
        }

        /**
         * Returns the name of the current type
         */
        public String getType() {
            return type;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            emitTypesDeprecationWarning();
            return new ConstantIndexFieldData.Builder(type, name(), CoreValuesSourceType.KEYWORD);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            emitTypesDeprecationWarning();
            return new MatchAllDocsQuery();
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, SearchExecutionContext context) {
            emitTypesDeprecationWarning();
            if (type == null) {
                return false;
            }
            return Regex.simpleMatch(pattern, type, caseInsensitive);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            ZoneId timeZone,
            DateMathParser parser,
            SearchExecutionContext context
        ) {
            emitTypesDeprecationWarning();
            BytesRef lower = (BytesRef) lowerTerm;
            BytesRef upper = (BytesRef) upperTerm;
            if (includeLower) {
                if (lower.utf8ToString().compareTo(type) > 0) {
                    return new MatchNoDocsQuery();
                }
            } else {
                if (lower.utf8ToString().compareTo(type) >= 0) {
                    return new MatchNoDocsQuery();
                }
            }
            if (includeUpper) {
                if (upper.utf8ToString().compareTo(type) < 0) {
                    return new MatchNoDocsQuery();
                }
            } else {
                if (upper.utf8ToString().compareTo(type) <= 0) {
                    return new MatchNoDocsQuery();
                }
            }
            return new MatchAllDocsQuery();
        }

        /**
         * Build a type filter
         *
         * This does not emit a deprecation warning, as it is only called when a type
         * has been specified in a REST request and warnings will have already been
         * emitted at the REST layer.
         */
        public Query typeFilter(String[] types) {
            for (String t : types) {
                if (Objects.equals(this.type, t)) {
                    return new MatchAllDocsQuery();
                }
            }
            return new MatchNoDocsQuery();
        }
    }

    private TypeFieldMapper() {
        super(new TypeFieldType(null));
    }

    @Override
    public void preParse(DocumentParserContext context) {

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
