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
 */
public abstract class GeoPointValues {

    public static final GeoPointValues EMPTY = new Empty();
    
    private final boolean multiValued;

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    public final boolean isMultiValued() {
        return multiValued;
    }

    /**
     * Is there a value for this doc?
     */
    public abstract boolean hasValue(int docId);

    public abstract GeoPoint getValue(int docId);

    public abstract GeoPoint getValueSafe(int docId);

    public abstract Iter getIter(int docId);

    public abstract Iter getIterSafe(int docId);
    
    protected GeoPointValues(boolean multiValued) {
        this.multiValued = multiValued;
    }

    public GeoPoint getValueMissing(int docId, GeoPoint defaultGeoPoint) {
        if (hasValue(docId)) {
            return getValue(docId);
        } 
        return defaultGeoPoint;
    }


    public static interface Iter {

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

    static class Empty extends GeoPointValues {
        protected Empty() {
            super(false);
        }

        public boolean hasValue(int docId) {
            return false;
        }

        public GeoPoint getValueSafe(int docId) {
            return getValue(docId);
        }

        public Iter getIterSafe(int docId) {
            return getIter(docId);
        }


        public GeoPoint getValue(int docId) {
            return null;
        }

        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }
    }
}
