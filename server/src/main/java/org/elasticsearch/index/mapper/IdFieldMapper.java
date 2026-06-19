/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A mapper for the _id field.
 */
public abstract class IdFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static final TypeParser PARSER = new ConfigurableTypeParser(mappingParserContext -> {
        var indexMode = mappingParserContext.getIndexSettings().getMode();
        if (indexMode == IndexMode.TIME_SERIES) {
            return new ConstantBuilder(TsidExtractingIdFieldMapper.INSTANCE);
        } else {
            boolean useColumnarIdByDefault = mappingParserContext.getIndexSettings().isUseColumnarIdByDefault();
            return new ProvidedIdFieldMapper.Builder(useColumnarIdByDefault);
        }
    }) {

        @Override
        public Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException {
            var indexMode = parserContext.getIndexSettings().getMode();
            if (indexMode == IndexMode.TIME_SERIES) {
                throw new MapperParsingException(name + " is not configurable if index mode is time_series");
            } else {
                return super.parse(name, node, parserContext);
            }
        }
    };

    private static final Map<String, NamedAnalyzer> ANALYZERS = Map.of(NAME, Lucene.KEYWORD_ANALYZER);

    protected IdFieldMapper(MappedFieldType mappedFieldType) {
        super(mappedFieldType);
        assert mappedFieldType.isSearchable();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return ANALYZERS;
    }

    @Override
    protected final String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Description of the document being parsed used in error messages. Not
     * called unless there is an error.
     */
    public abstract String documentDescription(DocumentParserContext context);

    /**
     * Description of the document being indexed used after parsing for things
     * like version conflicts.
     */
    public abstract String documentDescription(ParsedDocument parsedDocument);

    /**
     * Returns {@code true} when the {@code _id} is stored as sorted doc values rather than a stored field.
     */
    public boolean isColumnarMode() {
        return false;
    }

    /**
     * Create an indexed and stored {@link Field} for the provided {@code _id}.
     */
    public static Field standardIdField(String id) {
        return standardIdField(Uid.encodeId(id), Field.Store.YES);
    }

    /**
     * Create an indexed {@link Field} for the provided {@code _id}, optionally stored.
     * The id must already be encoded using {@link Uid#encodeId(String)}.
     */
    public static Field standardIdField(BytesRef uid, Field.Store stored) {
        return new StringField(NAME, uid, stored);
    }

    /**
     * Create a {@link Field} corresponding to a synthetic {@code _id} field, which is not indexed and not stored but instead computed at
     * runtime.
     */
    public static Field syntheticIdField(String id) {
        return new SyntheticIdField(Uid.encodeId(id));
    }

    /**
     * Create a {@link Field} corresponding to a synthetic {@code _id} field, which is not indexed and not stored but instead resolved at
     * runtime. The id must be already encoded using {@link Uid#encodeId(String)}.
     */
    public static Field syntheticIdField(BytesRef uid) {
        return new SyntheticIdField(uid);
    }

    protected abstract static class AbstractIdFieldType extends TermBasedFieldType {

        public AbstractIdFieldType() {
            this(false);
        }

        public AbstractIdFieldType(boolean hasDocValues) {
            super(NAME, IndexType.terms(true, hasDocValues), true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // The _id field is always searchable.
            return true;
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            List<BytesRef> bytesRefs = values.stream().map(v -> {
                Object idObject = v;
                if (idObject instanceof BytesRef) {
                    idObject = ((BytesRef) idObject).utf8ToString();
                }
                return Uid.encodeId(idObject.toString());
            }).toList();
            return new TermInSetQuery(name(), bytesRefs);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return termsQuery(Arrays.asList(value), context);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return Queries.ALL_DOCS_INSTANCE;
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return IdLoader.create(blContext.indexSettings(), blContext.mappingLookup()).blockLoader(blContext.ordinalsByteSize());
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }
}
