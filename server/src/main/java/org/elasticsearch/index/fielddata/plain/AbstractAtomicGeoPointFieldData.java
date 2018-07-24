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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractAtomicGeoPointFieldData implements AtomicGeoPointFieldData {

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getGeoPointValues());
    }

    @Override
    public final ScriptDocValues.GeoPoints getScriptValues() {
        return new ScriptDocValues.GeoPoints(getGeoPointValues());
    }

    public static AtomicGeoPointFieldData empty(final int maxDoc) {
        return new AbstractAtomicGeoPointFieldData() {

            @Override
            public long ramBytesUsed() {
                return 0;
            }
            
            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public MultiGeoPointValues getGeoPointValues() {
                return FieldData.emptyMultiGeoPoints();
            }
        };
    }
}
