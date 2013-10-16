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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

/**
 * A state-full lightweight per document set of <code>long</code> values.
 *
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   LongValues values = ..;
 *   final int numValues = values.setDocId(docId);
 *   for (int i = 0; i < numValues; i++) {
 *       long value = values.nextValue();
 *       // process value
 *   }
 * </pre>
 *
 */
public abstract class LongValues {

    /**
     * An empty {@link LongValues instance}
     */
    public static final LongValues EMPTY = new Empty();

    private final boolean multiValued;

    protected int docId;

    /**
     * Creates a new {@link LongValues} instance
     * @param multiValued <code>true</code> iff this instance is multivalued. Otherwise <code>false</code>.
     */
    protected LongValues(boolean multiValued) {
        this.multiValued = multiValued;
    }

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    public final boolean isMultiValued() {
        return multiValued;
    }

    /**
     * Returns <code>true</code> if the given document ID has a value in this. Otherwise <code>false</code>.
     */
    public abstract boolean hasValue(int docId);

    /**
     * Returns a value for the given document id. If the document
     * has more than one value the returned value is one of the values
     * associated with the document.
     * @param docId the documents id.
     * @return a value for the given document id.
     */
    public abstract long getValue(int docId);

    /**
     * Returns a value for the given document id or the given missing value if
     * {@link #hasValue(int)} returns <code>false</code> ie. the document has no
     * value associated with it.
     *
     * @param docId        the documents id.
     * @param missingValue the missing value
     * @return a value for the given document id or the given missing value if
     *         {@link #hasValue(int)} returns <code>false</code> ie. the document has no
     *         value associated with it.
     */
    public long getValueMissing(int docId, long missingValue) {
        if (hasValue(docId)) {
            return getValue(docId);
        }
        return missingValue;
    }

    /**
     * Sets iteration to the specified docID and returns the number of
     * values for this document ID,
     * @param docId document ID
     *
     * @see #nextValue()
     */
    public int setDocument(int docId) {
        this.docId = docId;
        return hasValue(docId) ?  1 : 0;
    }

    /**
     * Returns the next value for the current docID set to {@link #setDocument(int)}.
     * This method should only be called <tt>N</tt> times where <tt>N</tt> is the number
     * returned from {@link #setDocument(int)}. If called more than <tt>N</tt> times the behavior
     * is undefined.
     *
     * @return the next value for the current docID set to {@link #setDocument(int)}.
     */
    public long nextValue() {
        return getValue(docId);
    }

    /**
     * Ordinal based {@link LongValues}.
     */
    public static abstract class WithOrdinals extends LongValues {

        protected final Docs ordinals;

        protected WithOrdinals(Ordinals.Docs ordinals) {
            super(ordinals.isMultiValued());
            this.ordinals = ordinals;
        }

        /**
         * Returns the associated ordinals instance.
         * @return the associated ordinals instance.
         */
        public Docs ordinals() {
            return this.ordinals;
        }

        /**
         * Returns the value for the given ordinal.
         * @param ord the ordinal to lookup.
         * @return a long value associated with the given ordinal.
         */
        public abstract long getValueByOrd(long ord);

        @Override
        public final boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != Ordinals.MISSING_ORDINAL;
        }

        @Override
        public final long getValue(int docId) {
            long ord = ordinals.getOrd(docId);
            if (ord == Ordinals.MISSING_ORDINAL) {
                return 0l;
            }
            return getValueByOrd(ord);
        }

        @Override
        public int setDocument(int docId) {
            this.docId = docId;
            return ordinals.setDocument(docId);
        }

        @Override
        public long nextValue() {
            return getValueByOrd(ordinals.nextOrd());
        }
    }

  
    private static final class Empty extends LongValues {

        public Empty() {
            super(false);
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public long getValue(int docId) {
            // conforms with all other impls when there is no value
            return 0;
        }
        
        @Override
        public int setDocument(int docId) {
            return 0;
        }
        
        @Override
        public long nextValue() {
            throw new ElasticSearchIllegalStateException("Empty LongValues has no next value");
        }

    }
}
