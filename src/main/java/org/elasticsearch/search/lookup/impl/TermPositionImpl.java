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

package org.elasticsearch.search.lookup.impl;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.search.lookup.TermPosition;

class TermPositionImpl extends TermPosition {

    BytesRef payload;
    private CharsRefBuilder spare = new CharsRefBuilder(); 
    
    public byte[] payload() {
        if (payload == null) {
            return null;
        } else {
            byte bytes[] = new byte[payload.length];
            System.arraycopy(payload.bytes, payload.offset, bytes, 0, bytes.length);
            return bytes;
        }
    }

    public String payloadAsString() {
        if (payload != null && payload.length != 0) {
            spare.copyUTF8Bytes(payload);
            return spare.toString();
        } else {
            return null;
        }
    }

    public float payloadAsFloat(float defaultMissing) {
        if (payload != null && payload.length != 0) {
            return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
        } else {
            return defaultMissing;
        }
    }

    public int payloadAsInt(int defaultMissing) {
        if (payload != null && payload.length != 0) {
            return PayloadHelper.decodeInt(payload.bytes, payload.offset);
        } else {
            return defaultMissing;
        }
    }
}
