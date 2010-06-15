/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.trove.TObjectIntHashMap;
import org.elasticsearch.common.trove.TObjectIntIterator;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
class ShardTermsResponse extends BroadcastShardOperationResponse {

    private Map<String, TObjectIntHashMap<Object>> fieldsTermsFreqs = new HashMap<String, TObjectIntHashMap<Object>>();

    private int numDocs;

    private int maxDoc;

    private int numDeletedDocs;

    ShardTermsResponse() {
    }

    ShardTermsResponse(String index, int shardId, int numDocs, int maxDoc, int numDeletedDocs) {
        super(index, shardId);
        this.numDocs = numDocs;
        this.maxDoc = maxDoc;
        this.numDeletedDocs = numDeletedDocs;
    }

    int numDocs() {
        return this.numDocs;
    }

    int maxDoc() {
        return this.maxDoc;
    }

    int numDeletedDocs() {
        return this.numDeletedDocs;
    }

    void put(String fieldName, TObjectIntHashMap<Object> termsFreqs) {
        fieldsTermsFreqs.put(fieldName, termsFreqs);
    }

    Map<String, TObjectIntHashMap<Object>> fieldsTermsFreqs() {
        return fieldsTermsFreqs;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numDocs = in.readVInt();
        maxDoc = in.readVInt();
        numDeletedDocs = in.readVInt();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String fieldName = in.readUTF();

            TObjectIntHashMap<Object> termsFreq = new TObjectIntHashMap<Object>();
            int size1 = in.readVInt();
            for (int j = 0; j < size1; j++) {
                termsFreq.put(Lucene.readFieldValue(in), in.readVInt());
            }

            fieldsTermsFreqs.put(fieldName, termsFreq);
        }
    }

    @Override public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numDocs);
        out.writeVInt(maxDoc);
        out.writeVInt(numDeletedDocs);
        out.writeVInt(fieldsTermsFreqs.size());
        for (Map.Entry<String, TObjectIntHashMap<Object>> entry : fieldsTermsFreqs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (TObjectIntIterator<Object> it = entry.getValue().iterator(); it.hasNext();) {
                it.advance();
                Lucene.writeFieldValue(out, it.key());
                out.writeVInt(it.value());
            }
        }
    }
}
