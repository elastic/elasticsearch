/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.features.NodeFeature;
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

public class IndexModeFieldMapper extends MetadataFieldMapper {

    static final NodeFeature QUERYING_INDEX_MODE = new NodeFeature("mapper.query_index_mode");

    public static final String NAME = "_index_mode";

    public static final String CONTENT_TYPE = "_index_mode";

    private static final IndexModeFieldMapper INSTANCE = new IndexModeFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    static final class IndexModeFieldType extends ConstantFieldType {

        static final IndexModeFieldType INSTANCE = new IndexModeFieldType();

        private IndexModeFieldType() {
            super(NAME, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, QueryRewriteContext context) {
            final String indexMode = context.getIndexSettings().getMode().getName();
            return Regex.simpleMatch(pattern, indexMode, caseInsensitive);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            final String indexMode = fieldDataContext.indexSettings().getMode().getName();
            return new ConstantIndexFieldData.Builder(
                indexMode,
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
            final String indexMode = blContext.indexSettings().getMode().getName();
            return BlockLoader.constantBytes(new BytesRef(indexMode));
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new ValueFetcher() {
                private final List<Object> indexMode = List.of(context.getIndexSettings().getMode().getName());

                @Override
                public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
                    return indexMode;
                }

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }
            };
        }

    }

    public IndexModeFieldMapper() {
        super(IndexModeFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
