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

package org.elasticsearch.index.mapper.json;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.util.lucene.search.TermFilter;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class JsonFieldMapper<T> implements FieldMapper<T>, JsonMapper {

    public static class Defaults {
        public static final Field.Index INDEX = Field.Index.ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final Field.TermVector TERM_VECTOR = Field.TermVector.NO;
        public static final float BOOST = 1.0f;
        public static final boolean OMIT_NORMS = false;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = false;
    }

    public abstract static class Builder<T extends Builder, Y extends JsonFieldMapper> extends JsonMapper.Builder<T, Y> {

        protected Field.Index index = Defaults.INDEX;

        protected Field.Store store = Defaults.STORE;

        protected Field.TermVector termVector = Defaults.TERM_VECTOR;

        protected float boost = Defaults.BOOST;

        protected boolean omitNorms = Defaults.OMIT_NORMS;

        protected boolean omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;

        protected String indexName;

        protected Analyzer indexAnalyzer;

        protected Analyzer searchAnalyzer;

        public Builder(String name) {
            super(name);
            indexName = name;
        }

        public T index(Field.Index index) {
            this.index = index;
            return builder;
        }

        public T store(Field.Store store) {
            this.store = store;
            return builder;
        }

        public T termVector(Field.TermVector termVector) {
            this.termVector = termVector;
            return builder;
        }

        public T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        public T omitNorms(boolean omitNorms) {
            this.omitNorms = omitNorms;
            return builder;
        }

        public T omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
            this.omitTermFreqAndPositions = omitTermFreqAndPositions;
            return builder;
        }

        public T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        public T indexAnalyzer(Analyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            if (this.searchAnalyzer == null) {
                this.searchAnalyzer = indexAnalyzer;
            }
            return builder;
        }

        public T searchAnalyzer(Analyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        protected String buildIndexName(BuilderContext context) {
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    protected final String name;

    protected final String indexName;

    protected final String fullName;

    protected final Field.Index index;

    protected final Field.Store store;

    protected final Field.TermVector termVector;

    protected final float boost;

    protected final boolean omitNorms;

    protected final boolean omitTermFreqAndPositions;

    protected final Analyzer indexAnalyzer;

    protected final Analyzer searchAnalyzer;

    protected JsonFieldMapper(String name, String indexName, String fullName, Field.Index index, Field.Store store, Field.TermVector termVector,
                              float boost, boolean omitNorms, boolean omitTermFreqAndPositions, Analyzer indexAnalyzer, Analyzer searchAnalyzer) {
        this.name = name;
        this.indexName = indexName;
        this.fullName = fullName;
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
        return this.name;
    }

    @Override public String indexName() {
        return this.indexName;
    }

    @Override public String fullName() {
        return this.fullName;
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

    @Override public void parse(JsonParseContext jsonContext) throws IOException {
        if (!indexed() && !stored()) {
            return;
        }
        Field field = parseCreateField(jsonContext);
        if (field == null) {
            return;
        }
        field.setOmitNorms(omitNorms);
        field.setOmitTermFreqAndPositions(omitTermFreqAndPositions);
        field.setBoost(boost);
        jsonContext.doc().add(field);
    }

    protected abstract Field parseCreateField(JsonParseContext jsonContext) throws IOException;

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }

    @Override public Object valueForSearch(Fieldable field) {
        return valueAsString(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public String indexedValue(T value) {
        return value.toString();
    }

    @Override public Query fieldQuery(String value) {
        return new TermQuery(new Term(indexName, indexedValue(value)));
    }

    @Override public Filter fieldFilter(String value) {
        return new TermFilter(new Term(indexName, indexedValue(value)));
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return new TermRangeQuery(indexName,
                lowerTerm == null ? null : indexedValue(lowerTerm),
                upperTerm == null ? null : indexedValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return new TermRangeFilter(indexName,
                lowerTerm == null ? null : indexedValue(lowerTerm),
                upperTerm == null ? null : indexedValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override public int sortType() {
        return SortField.STRING;
    }
}
