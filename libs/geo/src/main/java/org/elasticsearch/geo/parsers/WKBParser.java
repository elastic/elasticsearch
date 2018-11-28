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

package org.elasticsearch.geo.parsers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.elasticsearch.geo.geometry.GeoShape;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

public class WKBParser {

    // no instance:
    private WKBParser() {
    }

    public enum ByteOrder {
        XDR, // big endian
        NDR; // little endian

        public BytesRef toWKB() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (OutputStreamDataOutput out = new OutputStreamDataOutput(baos)) {
                out.writeVInt(this.ordinal());
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
            return new BytesRef(baos.toByteArray());
        }
    }
}
