/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class IndexFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_index";

    public static final String CONTENT_TYPE = "_index";

    private static final IndexFieldMapper INSTANCE = new IndexFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    static final class IndexFieldType extends ConstantFieldType {

        static final IndexFieldType INSTANCE = new IndexFieldType();

        private IndexFieldType() {
            super(NAME, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, QueryRewriteContext context) {
            if (caseInsensitive) {
                // Thankfully, all index names are lower-cased so we don't have to pass a case_insensitive mode flag
                // down to all the index name-matching logic. We just lower-case the search string
                pattern = Strings.toLowercaseAscii(pattern);
            }
            return context.indexMatches(pattern);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new ConstantIndexFieldData.Builder(
                fieldDataContext.fullyQualifiedIndexName(),
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return BlockLoader.constantBytes(new BytesRef(blContext.indexName()));
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new ValueFetcher() {

                private final List<Object> indexName = List.of(context.getFullyQualifiedIndex().getName());

                @Override
                public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
                    return indexName;
                }

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }
            };
        }

        @Override
        public Query wildcardLikeQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            SearchExecutionContext context
        ) {
            String indexName = context.getFullyQualifiedIndex().getName();
            return getWildcardLikeQuery(value, caseInsensitve, indexName);
        }

        @Override
        public Query wildcardLikeQuery(String value, boolean caseInsensitive, QueryRewriteContext context) {
            String indexName = context.getFullyQualifiedIndex().getName();
            return getWildcardLikeQuery(value, caseInsensitive, indexName);
        }

        private static Query getWildcardLikeQuery(String value, boolean caseInsensitve, String indexName) {
            if (caseInsensitve) {
                value = value.toLowerCase(Locale.ROOT);
                indexName = indexName.toLowerCase(Locale.ROOT);
            }
            if (Regex.simpleMatch(value, indexName)) {
                return new MatchAllDocsQuery();
            }
            return new MatchNoDocsQuery("The \"" + indexName + "\" query was rewritten to a \"match_none\" query.");
        }

        @Override
        public String getConstantFieldValue(SearchExecutionContext context) {
            return context.getFullyQualifiedIndex().getName();
        }
    }

    public IndexFieldMapper() {
        super(IndexFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
