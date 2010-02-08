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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.util.concurrent.ThreadSafe;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public interface FieldMapper<T> {

    /**
     * The name of the field (this is not what we store in the index).
     */
    String name();

    /**
     * The indexed name of the field. This is the name under which we will
     * store it in the index.
     */
    String indexName();

    /**
     * The full name of the field. If it is under a certain context (for example,
     * in json it exists within an object with a given name), then the context
     * will be included. Expected to end with the {@link #name()}.
     */
    String fullName();

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

    /**
     * Returns the actual value of the field as string.
     */
    String valueAsString(Fieldable field);

    /**
     * Returns the indexed value.
     */
    String indexedValue(String value);

    /**
     * Returns the indexed value.
     */
    String indexedValue(T value);

    Query fieldQuery(String value);

    Filter fieldFilter(String value);

    /**
     * Constructs a range query based on the mapper.
     */
    Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper);

    /**
     * Constructs a range query filter based on the mapper.
     */
    Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper);

    int sortType();
}
