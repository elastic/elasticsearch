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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
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
        public static final Field.Index INDEX = Field.Index.ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final Field.TermVector TERM_VECTOR = Field.TermVector.NO;
        public static final float BOOST = 1.0f;
        public static final boolean OMIT_NORMS = false;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = false;
    }

    public abstract static class OpenBuilder<T extends Builder, Y extends AbstractFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected OpenBuilder(String name) {
            super(name);
        }

        @Override
        public T index(Field.Index index) {
            return super.index(index);
        }

        @Override
        public T store(Field.Store store) {
            return super.store(store);
        }

        @Override
        public T termVector(Field.TermVector termVector) {
            return super.termVector(termVector);
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
        public T omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
            return super.omitTermFreqAndPositions(omitTermFreqAndPositions);
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

        protected Field.Index index = Defaults.INDEX;

        protected Field.Store store = Defaults.STORE;

        protected Field.TermVector termVector = Defaults.TERM_VECTOR;

        protected float boost = Defaults.BOOST;

        protected boolean omitNorms = Defaults.OMIT_NORMS;

        protected boolean omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;

        protected String indexName;

        protected NamedAnalyzer indexAnalyzer;

        protected NamedAnalyzer searchAnalyzer;

        protected Boolean includeInAll;

        protected Builder(String name) {
            super(name);
        }

        protected T index(Field.Index index) {
            this.index = index;
            return builder;
        }

        protected T store(Field.Store store) {
            this.store = store;
            return builder;
        }

        protected T termVector(Field.TermVector termVector) {
            this.termVector = termVector;
            return builder;
        }

        protected T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        protected T omitNorms(boolean omitNorms) {
            this.omitNorms = omitNorms;
            return builder;
        }

        protected T omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
            this.omitTermFreqAndPositions = omitTermFreqAndPositions;
            return builder;
        }

        protected T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        protected T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            if (this.searchAnalyzer == null) {
                this.searchAnalyzer = indexAnalyzer;
            }
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

    protected final Field.Index index;

    protected final Field.Store store;

    protected final Field.TermVector termVector;

    protected float boost;

    protected final boolean omitNorms;

    protected final boolean omitTermFreqAndPositions;
    protected final FieldInfo.IndexOptions indexOptions;

    protected final NamedAnalyzer indexAnalyzer;

    protected final NamedAnalyzer searchAnalyzer;

    protected AbstractFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                  float boost, boolean omitNorms, boolean omitTermFreqAndPositions, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        this.names = names;
        this.index = index;
        this.store = store;
        this.termVector = termVector;
        this.boost = boost;
        this.omitNorms = omitNorms;
        this.omitTermFreqAndPositions = omitTermFreqAndPositions;
        this.indexOptions = omitTermFreqAndPositions ? FieldInfo.IndexOptions.DOCS_ONLY : FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        if (indexAnalyzer == null && !index.isAnalyzed()) {
            this.indexAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.indexAnalyzer = indexAnalyzer;
        }
        if (searchAnalyzer == null && !index.isAnalyzed()) {
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
    public Field.Index index() {
        return this.index;
    }

    @Override
    public Field.Store store() {
        return this.store;
    }

    @Override
    public boolean stored() {
        return store == Field.Store.YES;
    }

    @Override
    public boolean indexed() {
        return index != Field.Index.NO;
    }

    @Override
    public boolean analyzed() {
        return index == Field.Index.ANALYZED;
    }

    @Override
    public Field.TermVector termVector() {
        return this.termVector;
    }

    @Override
    public float boost() {
        return this.boost;
    }

    @Override
    public boolean omitNorms() {
        return this.omitNorms;
    }

    @Override
    public boolean omitTermFreqAndPositions() {
        return this.omitTermFreqAndPositions;
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
    public void parse(ParseContext context) throws IOException {
        try {
            Fieldable field = parseCreateField(context);
            if (field == null) {
                return;
            }
            field.setOmitNorms(omitNorms);
            field.setIndexOptions(indexOptions);
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

    protected abstract Fieldable parseCreateField(ParseContext context) throws IOException;

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
    public Object valueForSearch(Fieldable field) {
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
        return new TermRangeQuery(names.indexName(),
                lowerTerm == null ? null : indexedValue(lowerTerm),
                upperTerm == null ? null : indexedValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeFilter(names.indexName(),
                lowerTerm == null ? null : indexedValue(lowerTerm),
                upperTerm == null ? null : indexedValue(upperTerm),
                includeLower, includeUpper);
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
        if (!this.index.equals(fieldMergeWith.index)) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index values");
        }
        if (!this.store.equals(fieldMergeWith.store)) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store values");
        }
        if (!this.termVector.equals(fieldMergeWith.termVector)) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different term_vector values");
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

    protected void doXContentBody(XContentBuilder builder) throws IOException {
        builder.field("type", contentType());
        if (!names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (indexAnalyzer != null && searchAnalyzer != null && indexAnalyzer.name().equals(searchAnalyzer.name()) && !indexAnalyzer.name().startsWith("_")) {
            // same analyzers, output it once
            builder.field("analyzer", indexAnalyzer.name());
        } else {
            if (indexAnalyzer != null && !indexAnalyzer.name().startsWith("_")) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
            if (searchAnalyzer != null && !searchAnalyzer.name().startsWith("_")) {
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
