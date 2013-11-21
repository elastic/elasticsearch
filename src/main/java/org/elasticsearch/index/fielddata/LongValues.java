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
     * is undefined.
     * <p>
     * If this instance returns ordered values the <tt>Nth</tt> value is strictly less than the <tt>N+1</tt> value with
     * respect to the {@link AtomicFieldData.Order} returned from {@link #getOrder()}. If this instance returns
     * <i>unordered</i> values {@link #getOrder()} must return {@link AtomicFieldData.Order#NONE}
     * Note: the values returned are de-duplicated, only unique values are returned.
     * </p>
     *
     * @return the next value for the current docID set to {@link #setDocument(int)}.
     */
    public abstract long nextValue();

    /**
     * Returns the order the values are returned from {@link #nextValue()}.
     * <p> Note: {@link LongValues} have {@link AtomicFieldData.Order#NUMERIC} by default.</p>
     */
    public AtomicFieldData.Order getOrder() {
        return AtomicFieldData.Order.NUMERIC;
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
        public int setDocument(int docId) {
            return 0;
        }
        
        @Override
        public long nextValue() {
            throw new ElasticSearchIllegalStateException("Empty LongValues has no next value");
        }

    }
}
