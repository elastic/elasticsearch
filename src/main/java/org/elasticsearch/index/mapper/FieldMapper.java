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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.query.QueryParseContext;

/**
 *
 */
public interface FieldMapper<T> {

    public static class Names {

        private final String name;

        private final String indexName;

        private final String indexNameClean;

        private final String fullName;

        private final String sourcePath;

        private final Term indexNameTermFactory;

        public Names(String name) {
            this(name, name, name, name);
        }

        public Names(String name, String indexName, String indexNameClean, String fullName) {
            this(name, indexName, indexNameClean, fullName, fullName);
        }

        public Names(String name, String indexName, String indexNameClean, String fullName, @Nullable String sourcePath) {
            this.name = name.intern();
            this.indexName = indexName.intern();
            this.indexNameClean = indexNameClean.intern();
            this.fullName = fullName.intern();
            this.sourcePath = sourcePath == null ? this.fullName : sourcePath.intern();
            this.indexNameTermFactory = new Term(indexName, "");
        }

        /**
         * The logical name of the field.
         */
        public String name() {
            return name;
        }

        /**
         * The indexed name of the field. This is the name under which we will
         * store it in the index.
         */
        public String indexName() {
            return indexName;
        }

        /**
         * The cleaned index name, before any "path" modifications performed on it.
         */
        public String indexNameClean() {
            return indexNameClean;
        }

        /**
         * The full name, including dot path.
         */
        public String fullName() {
            return fullName;
        }

        /**
         * The dot path notation to extract the value from source.
         */
        public String sourcePath() {
            return sourcePath;
        }

        /**
         * The index name term that can be used as a factory.
         */
        public Term indexNameTerm() {
            return this.indexNameTermFactory;
        }

        /**
         * Creates a new index term based on the provided value.
         */
        public Term createIndexNameTerm(String value) {
            return indexNameTermFactory.createTerm(value);
        }
    }

    Names names();

    Field.Index index();

    boolean indexed();

    boolean analyzed();

    Field.Store store();

    boolean stored();

    Field.TermVector termVector();

    float boost();

    boolean omitNorms();

    boolean omitTermFreqAndPositions();

    /**
     * The analyzer that will be used to index the field.
     */
    Analyzer indexAnalyzer();

    /**
     * The analyzer that will be used to search the field.
     */
    Analyzer searchAnalyzer();

    /**
     * Returns the value that will be used as a result for search. Can be only of specific types... .
     */
    Object valueForSearch(Fieldable field);

    /**
     * Returns the actual value of the field.
     */
    T value(Fieldable field);

    T valueFromString(String value);

    /**
     * Returns the actual value of the field as string.
     */
    String valueAsString(Fieldable field);

    /**
     * Returns the indexed value.
     */
    String indexedValue(String value);

    /**
     * Should the field query {@link #fieldQuery(String, org.elasticsearch.index.query.QueryParseContext)}  be used when detecting this
     * field in query string.
     */
    boolean useFieldQueryWithQueryString();

    /**
     * A field query for the specified value.
     */
    Query fieldQuery(String value, @Nullable QueryParseContext context);

    Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions);

    Query fuzzyQuery(String value, double minSim, int prefixLength, int maxExpansions);

    Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context);

    Filter prefixFilter(String value, @Nullable QueryParseContext context);

    /**
     * A term query to use when parsing a query string. Can return <tt>null</tt>.
     */
    Query queryStringTermQuery(Term term);

    Filter fieldFilter(String value, @Nullable QueryParseContext context);

    /**
     * Constructs a range query based on the mapper.
     */
    Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    /**
     * Constructs a range query filter based on the mapper.
     */
    Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    FieldDataType fieldDataType();
}
