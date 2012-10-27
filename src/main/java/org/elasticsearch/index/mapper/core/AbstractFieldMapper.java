/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;

/**
 *
 */
public abstract class AbstractFieldMapper<T> implements FieldMapper<T>, Mapper {

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }

        public static final float BOOST = 1.0f;
    }

    public abstract static class OpenBuilder<T extends Builder, Y extends AbstractFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected OpenBuilder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        @Override
        public T index(boolean index) {
            return super.index(index);
        }

        @Override
        public T store(boolean store) {
            return super.store(store);
        }

        @Override
        protected T storeTermVectors(boolean termVectors) {
            return super.storeTermVectors(termVectors);
        }

        @Override
        protected T storeTermVectorOffsets(boolean termVectorOffsets) {
            return super.storeTermVectorOffsets(termVectorOffsets);
        }

        @Override
        protected T storeTermVectorPositions(boolean termVectorPositions) {
            return super.storeTermVectorPositions(termVectorPositions);
        }

        @Override
        protected T storeTermVectorPayloads(boolean termVectorPayloads) {
            return super.storeTermVectorPayloads(termVectorPayloads);
        }

        @Override
        protected T tokenized(boolean tokenized) {
            return super.tokenized(tokenized);
        }

        @Override
        public T boost(float boost) {
            return super.boost(boost);
        }

        @Override
        public T omitNorms(boolean omitNorms) {
            return super.omitNorms(omitNorms);
        }

        @Override
        public T indexOptions(IndexOptions indexOptions) {
            return super.indexOptions(indexOptions);
        }

        @Override
        public T indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override
        public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            return super.indexAnalyzer(indexAnalyzer);
        }

        @Override
        public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            return super.searchAnalyzer(searchAnalyzer);
        }
    }

    public abstract static class Builder<T extends Builder, Y extends AbstractFieldMapper> extends Mapper.Builder<T, Y> {

        protected final FieldType fieldType;
        protected float boost = Defaults.BOOST;
        protected boolean omitNormsSet = false;
        protected String indexName;
        protected NamedAnalyzer indexAnalyzer;
        protected NamedAnalyzer searchAnalyzer;
        protected Boolean includeInAll;
        protected boolean indexOptionsSet = false;

        protected Builder(String name, FieldType fieldType) {
            super(name);
            this.fieldType = fieldType;
        }

        protected T index(boolean index) {
            this.fieldType.setIndexed(index);
            return builder;
        }

        protected T store(boolean store) {
            this.fieldType.setStored(store);
            return builder;
        }

        protected T storeTermVectors(boolean termVectors) {
            this.fieldType.setStoreTermVectors(termVectors);
            return builder;
        }

        protected T storeTermVectorOffsets(boolean termVectorOffsets) {
            this.fieldType.setStoreTermVectors(termVectorOffsets);
            this.fieldType.setStoreTermVectorOffsets(termVectorOffsets);
            return builder;
        }

        protected T storeTermVectorPositions(boolean termVectorPositions) {
            this.fieldType.setStoreTermVectors(termVectorPositions);
            this.fieldType.setStoreTermVectorPositions(termVectorPositions);
            return builder;
        }

        protected T storeTermVectorPayloads(boolean termVectorPayloads) {
            this.fieldType.setStoreTermVectors(termVectorPayloads);
            this.fieldType.setStoreTermVectorPayloads(termVectorPayloads);
            return builder;
        }

        protected T tokenized(boolean tokenized) {
            this.fieldType.setTokenized(tokenized);
            return builder;
        }

        protected T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        protected T omitNorms(boolean omitNorms) {
            this.fieldType.setOmitNorms(omitNorms);
            this.omitNormsSet = true;
            return builder;
        }

        protected T indexOptions(IndexOptions indexOptions) {
            this.fieldType.setIndexOptions(indexOptions);
            this.indexOptionsSet = true;
            return builder;
        }

        protected T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        protected T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return builder;
        }

        protected T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        protected T includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        protected Names buildNames(BuilderContext context) {
            return new Names(name, buildIndexName(context), indexName == null ? name : indexName, buildFullName(context), context.path().sourcePath());
        }

        protected String buildIndexName(BuilderContext context) {
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    protected final Names names;

    protected float boost;

    protected final FieldType fieldType;

    protected final NamedAnalyzer indexAnalyzer;

    protected final NamedAnalyzer searchAnalyzer;

    protected AbstractFieldMapper(Names names, float boost, FieldType fieldType, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        this.names = names;
        this.boost = boost;
        this.fieldType = fieldType;
        this.fieldType.freeze();

        // automatically set to keyword analyzer if its indexed and not analyzed
        if (indexAnalyzer == null && !this.fieldType.tokenized() && this.fieldType.indexed()) {
            this.indexAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.indexAnalyzer = indexAnalyzer;
        }
        // automatically set to keyword analyzer if its indexed and not analyzed
        if (searchAnalyzer == null && !this.fieldType.tokenized() && this.fieldType.indexed()) {
            this.searchAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.searchAnalyzer = searchAnalyzer;
        }
    }

    @Override
    public String name() {
        return names.name();
    }

    @Override
    public Names names() {
        return this.names;
    }

    @Override
    public boolean stored() {
        return fieldType.stored();
    }

    @Override
    public boolean indexed() {
        return fieldType.indexed();
    }

    @Override
    public boolean analyzed() {
        return fieldType.tokenized();
    }

    @Override
    public boolean storeTermVectors() {
        return fieldType.storeTermVectors();
    }

    @Override
    public boolean storeTermVectorOffsets() {
        return fieldType.storeTermVectorOffsets();
    }

    @Override
    public boolean storeTermVectorPositions() {
        return fieldType.storeTermVectorPositions();
    }

    @Override
    public boolean storeTermVectorPayloads() {
        return fieldType.storeTermVectorPayloads();
    }

    @Override
    public float boost() {
        return this.boost;
    }

    @Override
    public boolean omitNorms() {
        return fieldType.omitNorms();
    }

    @Override
    public IndexOptions indexOptions() {
        return fieldType.indexOptions();
    }

    @Override
    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override
    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override
    public Analyzer searchQuoteAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Field field = parseCreateField(context);
            if (field == null) {
                return;
            }
            if (!customBoost()) {
                field.setBoost(boost);
            }
            if (context.listener().beforeFieldAdded(this, field, context)) {
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse [" + names.fullName() + "]", e);
        }
    }

    protected abstract Field parseCreateField(ParseContext context) throws IOException;

    /**
     * Derived classes can override it to specify that boost value is set by derived classes.
     */
    protected boolean customBoost() {
        return false;
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
        // nothing to do here...
    }

    @Override
    public Object valueForSearch(Field field) {
        return valueAsString(field);
    }

    @Override
    public String indexedValue(String value) {
        return value;
    }

    @Override
    public Query queryStringTermQuery(Term term) {
        return null;
    }

    @Override
    public boolean useFieldQueryWithQueryString() {
        return false;
    }

    @Override
    public Query fieldQuery(String value, @Nullable QueryParseContext context) {
        return new TermQuery(names().createIndexNameTerm(indexedValue(value)));
    }

    @Override
    public Filter fieldFilter(String value, @Nullable QueryParseContext context) {
        return new TermFilter(names().createIndexNameTerm(indexedValue(value)));
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions) {
        return new FuzzyQuery(names().createIndexNameTerm(indexedValue(value)), Float.parseFloat(minSim), prefixLength, maxExpansions);
    }

    @Override
    public Query fuzzyQuery(String value, double minSim, int prefixLength, int maxExpansions) {
        return new FuzzyQuery(names().createIndexNameTerm(value), (float) minSim, prefixLength, maxExpansions);
    }

    @Override
    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        PrefixQuery query = new PrefixQuery(names().createIndexNameTerm(indexedValue(value)));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Filter prefixFilter(String value, @Nullable QueryParseContext context) {
        return new PrefixFilter(names().createIndexNameTerm(indexedValue(value)));
    }

    @Override
    public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        // LUCENE 4 UPGRADE: Perhaps indexedValue() should return a BytesRef?
        return new TermRangeQuery(names.indexName(),
                lowerTerm == null ? null : new BytesRef(indexedValue(lowerTerm)),
                upperTerm == null ? null : new BytesRef(indexedValue(upperTerm)),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeFilter(names.indexName(),
                lowerTerm == null ? null : new BytesRef(indexedValue(lowerTerm)),
                upperTerm == null ? null : new BytesRef(indexedValue(upperTerm)),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        return null;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (!this.getClass().equals(mergeWith.getClass())) {
            String mergedType = mergeWith.getClass().getSimpleName();
            if (mergeWith instanceof AbstractFieldMapper) {
                mergedType = ((AbstractFieldMapper) mergeWith).contentType();
            }
            mergeContext.addConflict("mapper [" + names.fullName() + "] of different type, current_type [" + contentType() + "], merged_type [" + mergedType + "]");
            // different types, return
            return;
        }
        AbstractFieldMapper fieldMergeWith = (AbstractFieldMapper) mergeWith;
        if (this.indexed() != fieldMergeWith.indexed() || this.analyzed() != fieldMergeWith.analyzed()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index values");
        }
        if (this.stored() != fieldMergeWith.stored()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store values");
        }
        if (this.analyzed() != fieldMergeWith.analyzed()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different tokenize values");
        }
        if (this.storeTermVectors() != fieldMergeWith.storeTermVectors()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector values");
        }
        if (this.storeTermVectorOffsets() != fieldMergeWith.storeTermVectorOffsets()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_offsets values");
        }
        if (this.storeTermVectorPositions() != fieldMergeWith.storeTermVectorPositions()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_positions values");
        }
        if (this.storeTermVectorPayloads() != fieldMergeWith.storeTermVectorPayloads()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_payloads values");
        }
        if (this.indexAnalyzer == null) {
            if (fieldMergeWith.indexAnalyzer != null) {
                mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
            }
        } else if (fieldMergeWith.indexAnalyzer == null) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
        } else if (!this.indexAnalyzer.name().equals(fieldMergeWith.indexAnalyzer.name())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
        }
        if (this.searchAnalyzer == null) {
            if (fieldMergeWith.searchAnalyzer != null) {
                mergeContext.addConflict("mapper [" + names.fullName() + "] has different search_analyzer");
            }
        } else if (fieldMergeWith.searchAnalyzer == null) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different search_analyzer");
        } else if (!this.searchAnalyzer.name().equals(fieldMergeWith.searchAnalyzer.name())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different search_analyzer");
        }
        if (!mergeContext.mergeFlags().simulate()) {
            // apply changeable values
            this.boost = fieldMergeWith.boost;
        }
    }

    @Override
    public FieldDataType fieldDataType() {
        return FieldDataType.DefaultTypes.STRING;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(names.name());
        doXContentBody(builder);
        builder.endObject();
        return builder;
    }

    protected static String indexOptionToString(IndexOptions indexOption) {
        switch (indexOption) {
            case DOCS_AND_FREQS:
                return TypeParsers.INDEX_OPTIONS_FREQS;
            case DOCS_AND_FREQS_AND_POSITIONS:
                return TypeParsers.INDEX_OPTIONS_POSITIONS;
            case DOCS_ONLY:
                return TypeParsers.INDEX_OPTIONS_DOCS;
            default:
                throw new ElasticSearchIllegalArgumentException("Unknown IndexOptions [" + indexOption + "]");
        }
    }

    protected static String indexTokenizeOptionToString(boolean indexed, boolean tokenized) {
        if (!indexed) {
            return "no";
        } else if (tokenized) {
            return "analyzed";
        } else {
            return "not_analyzed";
        }
    }

    protected void doXContentBody(XContentBuilder builder) throws IOException {
        builder.field("type", contentType());
        if (!names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (indexAnalyzer != null && searchAnalyzer != null && indexAnalyzer.name().equals(searchAnalyzer.name()) && !indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default")) {
            // same analyzers, output it once
            builder.field("analyzer", indexAnalyzer.name());
        } else {
            if (indexAnalyzer != null && !indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default")) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
            if (searchAnalyzer != null && !searchAnalyzer.name().startsWith("_") && !searchAnalyzer.name().equals("default")) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        }
    }

    protected abstract String contentType();

    @Override
    public void close() {
        // nothing to do here, sub classes to override if needed
    }

}
