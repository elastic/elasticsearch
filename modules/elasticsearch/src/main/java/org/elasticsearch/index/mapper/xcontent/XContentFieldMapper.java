/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.util.lucene.search.TermFilter;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class XContentFieldMapper<T> implements FieldMapper<T>, XContentMapper {

    public static class Defaults {
        public static final Field.Index INDEX = Field.Index.ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final Field.TermVector TERM_VECTOR = Field.TermVector.NO;
        public static final float BOOST = 1.0f;
        public static final boolean OMIT_NORMS = false;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = false;
    }

    public abstract static class OpenBuilder<T extends Builder, Y extends XContentFieldMapper> extends XContentFieldMapper.Builder<T, Y> {

        protected OpenBuilder(String name) {
            super(name);
        }

        @Override public T index(Field.Index index) {
            return super.index(index);
        }

        @Override public T store(Field.Store store) {
            return super.store(store);
        }

        @Override public T termVector(Field.TermVector termVector) {
            return super.termVector(termVector);
        }

        @Override public T boost(float boost) {
            return super.boost(boost);
        }

        @Override public T omitNorms(boolean omitNorms) {
            return super.omitNorms(omitNorms);
        }

        @Override public T omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
            return super.omitTermFreqAndPositions(omitTermFreqAndPositions);
        }

        @Override public T indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            return super.indexAnalyzer(indexAnalyzer);
        }

        @Override public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            return super.searchAnalyzer(searchAnalyzer);
        }
    }

    public abstract static class Builder<T extends Builder, Y extends XContentFieldMapper> extends XContentMapper.Builder<T, Y> {

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
            return new Names(name, buildIndexName(context), indexName == null ? name : indexName, buildFullName(context));
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

    protected final float boost;

    protected final boolean omitNorms;

    protected final boolean omitTermFreqAndPositions;

    protected final NamedAnalyzer indexAnalyzer;

    protected final NamedAnalyzer searchAnalyzer;

    protected XContentFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                  float boost, boolean omitNorms, boolean omitTermFreqAndPositions, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        this.names = names;
        this.index = index;
        this.store = store;
        this.termVector = termVector;
        this.boost = boost;
        this.omitNorms = omitNorms;
        this.omitTermFreqAndPositions = omitTermFreqAndPositions;
        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
    }

    @Override public String name() {
        return names.name();
    }

    @Override public Names names() {
        return this.names;
    }

    @Override public Field.Index index() {
        return this.index;
    }

    @Override public Field.Store store() {
        return this.store;
    }

    @Override public boolean stored() {
        return store == Field.Store.YES;
    }

    @Override public boolean indexed() {
        return index != Field.Index.NO;
    }

    @Override public boolean analyzed() {
        return index == Field.Index.ANALYZED;
    }

    @Override public Field.TermVector termVector() {
        return this.termVector;
    }

    @Override public float boost() {
        return this.boost;
    }

    @Override public boolean omitNorms() {
        return this.omitNorms;
    }

    @Override public boolean omitTermFreqAndPositions() {
        return this.omitTermFreqAndPositions;
    }

    @Override public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override public void parse(ParseContext context) throws IOException {
        Fieldable field = parseCreateField(context);
        if (field == null) {
            return;
        }
        field.setOmitNorms(omitNorms);
        field.setOmitTermFreqAndPositions(omitTermFreqAndPositions);
        field.setBoost(boost);
        if (context.listener().beforeFieldAdded(this, field, context)) {
            context.doc().add(field);
        }
    }

    protected abstract Fieldable parseCreateField(ParseContext context) throws IOException;

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }

    @Override public Object valueForSearch(Fieldable field) {
        return valueAsString(field);
    }

    @Override public Object valueForSearch(Object value) {
        return value;
    }

    /**
     * Simply returns the same string.
     */
    @Override public Object valueFromTerm(String term) {
        return term;
    }

    @Override public Object valueFromString(String text) {
        return text;
    }

    /**
     * Never break on this term enumeration value.
     */
    @Override public boolean shouldBreakTermEnumeration(Object text) {
        return false;
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public String indexedValue(T value) {
        return value.toString();
    }

    @Override public Query queryStringTermQuery(Term term) {
        return null;
    }

    @Override public boolean useFieldQueryWithQueryString() {
        return false;
    }

    @Override public Query fieldQuery(String value) {
        return new TermQuery(new Term(names.indexName(), indexedValue(value)));
    }

    @Override public Filter fieldFilter(String value) {
        return new TermFilter(new Term(names.indexName(), indexedValue(value)));
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return new TermRangeQuery(names.indexName(),
                lowerTerm == null ? null : indexedValue(lowerTerm),
                upperTerm == null ? null : indexedValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return new TermRangeFilter(names.indexName(),
                lowerTerm == null ? null : indexedValue(lowerTerm),
                upperTerm == null ? null : indexedValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        mergeContext.addConflict("Mapper [" + names.fullName() + "] exists, can't merge");
    }

    @Override public int sortType() {
        return SortField.STRING;
    }

    @Override public FieldData.Type fieldDataType() {
        return FieldData.Type.STRING;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(names.name());
        doXContentBody(builder);
        builder.endObject();
    }

    protected void doXContentBody(XContentBuilder builder) throws IOException {
        builder.field("type", contentType());
        builder.field("index_name", names.indexNameClean());
        builder.field("index", index.name().toLowerCase());
        builder.field("store", store.name().toLowerCase());
        builder.field("term_vector", termVector.name().toLowerCase());
        builder.field("boost", boost);
        builder.field("omit_norms", omitNorms);
        builder.field("omit_term_freq_and_positions", omitTermFreqAndPositions);
        if (indexAnalyzer != null && !indexAnalyzer.name().startsWith("_")) {
            builder.field("index_analyzer", indexAnalyzer.name());
        }
        if (searchAnalyzer != null && !searchAnalyzer.name().startsWith("_")) {
            builder.field("search_analyzer", searchAnalyzer.name());
        }
    }

    protected abstract String contentType();
}
