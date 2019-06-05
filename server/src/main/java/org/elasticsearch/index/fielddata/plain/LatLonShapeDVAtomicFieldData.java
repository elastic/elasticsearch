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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeometryTreeReader;
import org.elasticsearch.index.fielddata.MultiGeoValues;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

final class LatLonShapeDVAtomicFieldData extends AbstractAtomicGeoShapeFieldData {
    private final LeafReader reader;
    private final String fieldName;

    LatLonShapeDVAtomicFieldData(LeafReader reader, String fieldName) {
        super();
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public MultiGeoValues getGeoValues() {
        try {
            final BinaryDocValues binaryValues = DocValues.getBinary(reader, fieldName);
            return new MultiGeoValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return binaryValues.advanceExact(doc);
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public GeoValue nextValue() throws IOException {
                    final BytesRef encoded = binaryValues.binaryValue();
                    return new GeoShapeValue(new GeometryTreeReader(encoded));
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
