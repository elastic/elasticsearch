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

import org.apache.lucene.util.Bits;
import org.elasticsearch.common.geo.GeoPoint;

final class SingletonMultiGeoPointValues extends MultiGeoPointValues {

    private final GeoPointValues in;
    private final Bits docsWithField;
    private GeoPoint value;
    private int count;

    SingletonMultiGeoPointValues(GeoPointValues in, Bits docsWithField) {
        this.in = in;
        this.docsWithField = docsWithField;
    }

    @Override
    public void setDocument(int docID) {
        value = in.get(docID);
        if (value.lat() == 0 && value.lon() == 0 && docsWithField != null && !docsWithField.get(docID)) {
            count = 0;
        } else {
            count = 1;
        }
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public GeoPoint valueAt(int index) {
        assert index == 0;
        return value;
    }

    public GeoPointValues getGeoPointValues() {
        return in;
    }

    public Bits getDocsWithField() {
        return docsWithField;
    }

}
