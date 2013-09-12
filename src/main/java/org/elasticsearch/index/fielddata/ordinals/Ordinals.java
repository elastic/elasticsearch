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

    /**
     * Are the ordinals backed by a single ordinals array?
     */
    boolean hasSingleArrayBackingStorage();

    /**
     * Returns the backing storage for this ordinals.
     */
    Object getBackingStorage();

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
     * The number of ordinals, excluding the "0" ordinal indicating a missing value.
     */
    long getNumOrds();

    /**
     * Returns total unique ord count; this includes +1 for
     * the null ord (always 0).
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
         * Returns an iterator of the ordinals that match the docId, with an
         * empty iterator for a doc with no ordinals.
         */
        Iter getIter(int docId);


        /**
         * An iterator over ordinals values.
         */
        interface Iter {

            /**
             * Gets the next ordinal. Returning 0 if the iteration is exhausted.
             */
            long next();
        }

        static class EmptyIter implements Iter {

            public static EmptyIter INSTANCE = new EmptyIter();

            @Override
            public long next() {
                return 0;
            }
        }

        static class SingleValueIter implements Iter {

            private long value;

            public SingleValueIter reset(long value) {
                this.value = value;
                return this;
            }

            @Override
            public long next() {
                long actual = value;
                value = 0;
                return actual;
            }
        }

    }

}
