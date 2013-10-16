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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.LongsRef;

/**
 * A thread safe ordinals abstraction. Ordinals can only be positive integers.
 */
public interface Ordinals {

    static final long MISSING_ORDINAL = 0;
    static final long MIN_ORDINAL = 1;

    /**
     * The memory size this ordinals take.
     */
    long getMemorySizeInBytes();

    /**
     * Is one of the docs maps to more than one ordinal?
     */
    boolean isMultiValued();

    /**
     * The number of docs in this ordinals.
     */
    int getNumDocs();

    /**
     * The number of ordinals, excluding the {@link #MISSING_ORDINAL} ordinal indicating a missing value.
     */
    long getNumOrds();

    /**
     * Returns total unique ord count; this includes +1 for
     * the  {@link #MISSING_ORDINAL}  ord (always  {@value #MISSING_ORDINAL} ).
     */
    long getMaxOrd();

    /**
     * Returns a lightweight (non thread safe) view iterator of the ordinals.
     */
    Docs ordinals();

    /**
     * A non thread safe ordinals abstraction, yet very lightweight to create. The idea
     * is that this gets created for each "iteration" over ordinals.
     * <p/>
     * <p>A value of 0 ordinal when iterating indicated "no" value.</p>
     * To iterate of a set of ordinals for a given document use {@link #setDocument(int)} and {@link #nextOrd()} as
     * show in the example below:
     * <pre>
     *   Ordinals.Docs docs = ...;
     *   final int len = docs.setDocId(docId);
     *   for (int i = 0; i < len; i++) {
     *       final long ord = docs.nextOrd();
     *       // process ord
     *   }
     * </pre>
     */
    interface Docs {

        /**
         * Returns the original ordinals used to generate this Docs "itereator".
         */
        Ordinals ordinals();

        /**
         * The number of docs in this ordinals.
         */
        int getNumDocs();

        /**
         * The number of ordinals, excluding the "0" ordinal (indicating a missing value).
         */
        long getNumOrds();

        /**
         * Returns total unique ord count; this includes +1 for
         * the null ord (always 0).
         */
        long getMaxOrd();

        /**
         * Is one of the docs maps to more than one ordinal?
         */
        boolean isMultiValued();

        /**
         * The ordinal that maps to the relevant docId. If it has no value, returns
         * <tt>0</tt>.
         */
        long getOrd(int docId);

        /**
         * Returns an array of ordinals matching the docIds, with 0 length one
         * for a doc with no ordinals.
         */
        LongsRef getOrds(int docId);


        /**
         * Returns the next ordinal for the current docID set to {@link #setDocument(int)}.
         * This method should only be called <tt>N</tt> times where <tt>N</tt> is the number
         * returned from {@link #setDocument(int)}. If called more than <tt>N</tt> times the behavior
         * is undefined.
         *
         * Note: This method will never return <tt>0</tt>.
         *
         * @return the next ordinal for the current docID set to {@link #setDocument(int)}.
         */
        long nextOrd();


        /**
         * Sets iteration to the specified docID and returns the number of
         * ordinals for this document ID,
         * @param docId document ID
         *
         * @see #nextOrd()
         */
        int setDocument(int docId);


        /**
         * Returns the current ordinal in the iteration
         * @return the current ordinal in the iteration
         */
        long currentOrd();
    }

}
