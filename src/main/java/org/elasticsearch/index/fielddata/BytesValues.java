/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.fielddata.plain.BytesValuesWithOrdinalsTermsEnum;

/**
 * A state-full lightweight per document set of <code>byte[]</code> values.
 *
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   BytesValues values = ..;
 *   final int numValues = values.setDocId(docId);
 *   for (int i = 0; i < numValues; i++) {
 *       BytesRef value = values.nextValue();
 *       // process value
 *   }
 * </pre>
 */
public abstract class BytesValues {

    /**
     * An empty {@link BytesValues instance}
     */
    public static final BytesValues EMPTY = new Empty();

    private final boolean multiValued;

    /**
     * Creates a new {@link BytesValues} instance
     * @param multiValued <code>true</code> iff this instance is multivalued. Otherwise <code>false</code>.
     */
    protected BytesValues(boolean multiValued) {
        this.multiValued = multiValued;
    }

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    public final boolean isMultiValued() {
        return multiValued;
    }

    /**
     * Sets iteration to the specified docID and returns the number of
     * values for this document ID,
     * @param docId document ID
     *
     * @see #nextValue()
     */
    public abstract int setDocument(int docId);

    /**
     * Returns the next value for the current docID set to {@link #setDocument(int)}.
     * This method should only be called <tt>N</tt> times where <tt>N</tt> is the number
     * returned from {@link #setDocument(int)}. If called more than <tt>N</tt> times the behavior
     * is undefined. This interface guarantees that the values are returned in order.
     * <p>
     * If this instance returns ordered values the <tt>Nth</tt> value is strictly less than the <tt>N+1</tt> value with
     * respect to the {@link AtomicFieldData.Order} returned from {@link #getOrder()}. If this instance returns
     * <i>unordered</i> values {@link #getOrder()} must return {@link AtomicFieldData.Order#NONE}
     * Note: the values returned are de-duplicated, only unique values are returned.
     * </p>
     *
     * Note: the returned {@link BytesRef} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #setDocument(int)}.
     */
    public abstract BytesRef nextValue();

    /**
     * Returns the order the values are returned from {@link #nextValue()}.
     * <p> Note: {@link BytesValues} have {@link AtomicFieldData.Order#BYTES} by default.</p>
     */
    public AtomicFieldData.Order getOrder() {
        return AtomicFieldData.Order.BYTES;
    }

    /**
     * Ordinal based {@link BytesValues}.
     */
    public static abstract class WithOrdinals extends BytesValues {

        public static final long MIN_ORDINAL = 0;
        public static final long MISSING_ORDINAL = SortedSetDocValues.NO_MORE_ORDS;

        protected WithOrdinals(boolean multiValued) {
            super(multiValued);
        }

        /**
         * Returns total unique ord count;
         */
        public abstract long getMaxOrd();

        /**
         * The ordinal that maps to the relevant docId. If it has no value, returns
         * <tt>0</tt>.
         */
        public abstract long getOrd(int docId);

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
        public abstract long nextOrd();

        /**
         * Returns the value for the given ordinal.
         * @param ord the ordinal to lookup.
         * @return a shared {@link BytesRef} instance holding the value associated
         *         with the given ordinal or <code>null</code> if ordinal is <tt>0</tt>
         */
        public abstract BytesRef getValueByOrd(long ord);

        @Override
        public BytesRef nextValue() {
            return getValueByOrd(nextOrd());
        }

        /**
         * Returns a terms enum to iterate over all the underlying values.
         */
        public TermsEnum getTermsEnum() {
            return new BytesValuesWithOrdinalsTermsEnum(this);
        }
    }

    /**
     * An empty {@link BytesValues} implementation
     */
    private final static class Empty extends BytesValues {

        Empty() {
            super(false);
        }

        @Override
        public int setDocument(int docId) {
            return 0;
        }

        @Override
        public BytesRef nextValue() {
            throw new ElasticsearchIllegalStateException("Empty BytesValues has no next value");
        }

    }
}
