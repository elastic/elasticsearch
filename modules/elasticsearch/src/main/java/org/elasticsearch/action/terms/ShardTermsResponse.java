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
import org.elasticsearch.util.gnu.trove.TObjectIntHashMap;
import org.elasticsearch.util.gnu.trove.TObjectIntIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardTermsResponse extends BroadcastShardOperationResponse {

    private Map<String, TObjectIntHashMap<String>> fieldsTermsFreqs = new HashMap<String, TObjectIntHashMap<String>>();

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

    void put(String fieldName, TObjectIntHashMap<String> termsFreqs) {
        fieldsTermsFreqs.put(fieldName, termsFreqs);
    }

    Map<String, TObjectIntHashMap<String>> fieldsTermsFreqs() {
        return fieldsTermsFreqs;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        numDocs = in.readInt();
        maxDoc = in.readInt();
        numDeletedDocs = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String fieldName = in.readUTF();

            TObjectIntHashMap<String> termsFreq = new TObjectIntHashMap<String>();
            int size1 = in.readInt();
            for (int j = 0; j < size1; j++) {
                termsFreq.put(in.readUTF(), in.readInt());
            }

            fieldsTermsFreqs.put(fieldName, termsFreq);
        }
    }

    @Override public void writeTo(final DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(numDocs);
        out.writeInt(maxDoc);
        out.writeInt(numDeletedDocs);
        out.writeInt(fieldsTermsFreqs.size());
        for (Map.Entry<String, TObjectIntHashMap<String>> entry : fieldsTermsFreqs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (TObjectIntIterator<String> it = entry.getValue().iterator(); it.hasNext();) {
                out.writeUTF(it.key());
                out.writeInt(it.value());
                it.advance();
            }
        }
    }
}
