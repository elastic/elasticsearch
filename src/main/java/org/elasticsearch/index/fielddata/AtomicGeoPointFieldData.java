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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;

/**
 */
public abstract class AtomicGeoPointFieldData<Script extends ScriptDocValues> implements AtomicFieldData<Script> {

    public abstract GeoPointValues getGeoPointValues();

    @Override
    public BytesValues getBytesValues(boolean needsHashes) {
        final GeoPointValues values = getGeoPointValues();
        return new BytesValues(values.isMultiValued()) {

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return values.setDocument(docId);
            }

            @Override
            public BytesRef nextValue() {
                GeoPoint value = values.nextValue();
                scratch.copyChars(GeoHashUtils.encode(value.lat(), value.lon()));
                return scratch;
            }

        };
    }

}
