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
import org.elasticsearch.index.fielddata.util.GeoPointArrayRef;
import org.elasticsearch.index.mapper.geo.GeoPoint;

/**
 */
public interface GeoPointValues {

    static final GeoPointValues EMPTY = new Empty();

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    GeoPoint getValue(int docId);

    GeoPoint getValueSafe(int docId);

    GeoPointArrayRef getValues(int docId);

    Iter getIter(int docId);

    Iter getIterSafe(int docId);

    /**
     * Go over all the possible values in their geo point format for a specific doc.
     */
    void forEachValueInDoc(int docId, ValueInDocProc proc);

    /**
     * Go over all the possible values in their geo point format for a specific doc.
     */
    void forEachSafeValueInDoc(int docId, ValueInDocProc proc);

    public static interface ValueInDocProc {
        void onValue(int docId, GeoPoint value);

        void onMissing(int docId);
    }

    /**
     * Go over all the possible values in their geo point format for a specific doc.
     */
    void forEachLatLonValueInDoc(int docId, LatLonValueInDocProc proc);

    public static interface LatLonValueInDocProc {
        void onValue(int docId, double lat, double lon);

        void onMissing(int docId);
    }

    static interface Iter {

        boolean hasNext();

        GeoPoint next();

        static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public GeoPoint next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public GeoPoint value;
            public boolean done;

            public Single reset(GeoPoint value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public GeoPoint next() {
                assert !done;
                done = true;
                return value;
            }
        }
    }

    static class Empty implements GeoPointValues {
        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public GeoPoint getValueSafe(int docId) {
            return getValue(docId);
        }

        @Override
        public Iter getIterSafe(int docId) {
            return getIter(docId);
        }

        @Override
        public void forEachSafeValueInDoc(int docId, ValueInDocProc proc) {

        }

        @Override
        public void forEachLatLonValueInDoc(int docId, LatLonValueInDocProc proc) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public GeoPoint getValue(int docId) {
            throw new ElasticSearchIllegalStateException("Can't retrieve a value from an empty GeoPointValues");
        }

        @Override
        public GeoPointArrayRef getValues(int docId) {
            return GeoPointArrayRef.EMPTY;
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            proc.onMissing(docId);
        }
    }
}
