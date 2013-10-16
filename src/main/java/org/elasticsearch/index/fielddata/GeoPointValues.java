/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.geo.GeoPoint;

/**
 * A state-full lightweight per document set of {@link GeoPoint} values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   GeoPointValues values = ..;
 *   final int numValues = values.setDocId(docId);
 *   for (int i = 0; i < numValues; i++) {
 *       GeoPoint value = values.nextValue();
 *       // process value
 *   }
 * </pre>
 */
public abstract class GeoPointValues {

    /**
     * An empty {@link GeoPointValues instance}
     */
    public static final GeoPointValues EMPTY = new Empty();
    
    private final boolean multiValued;

    protected int docId = -1;

    /**
     * Creates a new {@link GeoPointValues} instance
     * @param multiValued <code>true</code> iff this instance is multivalued. Otherwise <code>false</code>.
     */
    protected GeoPointValues(boolean multiValued) {
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
     *
     * Note: the {@link GeoPoint} might be shared across invocations.
     *
     * @param docId the documents id.
     * @return a value for the given document id.
     */
    public abstract GeoPoint getValue(int docId);

    /**
     * Sets iteration to the specified docID and returns the number of
     * values for this document ID,
     * @param docId document ID
     *
     * @see #nextValue()
     */
    public int setDocument(int docId) {
        this.docId = docId;
        return hasValue(docId) ? 1 : 0;
    }
    /**
     * Returns the next value for the current docID set to {@link #setDocument(int)}.
     * This method should only be called <tt>N</tt> times where <tt>N</tt> is the number
     * returned from {@link #setDocument(int)}. If called more than <tt>N</tt> times the behavior
     * is undefined.
     *
     * Note: the returned {@link GeoPoint} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #setDocument(int)}.
     */
    public GeoPoint nextValue() {
        assert docId != -1;
        return getValue(docId);
    }

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
    public GeoPoint getValueMissing(int docId, GeoPoint missingValue) {
        if (hasValue(docId)) {
            return getValue(docId);
        } 
        return missingValue;
    }

    /**
     * An empty {@link GeoPointValues} implementation
     */
    private static final class Empty extends GeoPointValues {
        protected Empty() {
            super(false);
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public GeoPoint getValue(int docId) {
            return null;
        }

        @Override
        public int setDocument(int docId) {
            return 0;
        }

        @Override
        public GeoPoint nextValue() {
            throw new ElasticSearchIllegalStateException("Empty GeoPointValues has no next value");
        }

    }
}
