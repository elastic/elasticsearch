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

package org.elasticsearch.search.fetch;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.*;

/**
 * Shard level fetch base request. Holds all the info needed to execute a fetch.
 * Used with search scroll as the original request doesn't hold indices.
 */
public class ShardFetchRequest extends TransportRequest {

    private long id;

    private int[] docIds;

    private Map<String, int[]> namedDocIds;

    private int size;

    private ScoreDoc lastEmittedDoc;

    public ShardFetchRequest() {
    }

    public ShardFetchRequest(SearchScrollRequest request, long id, IntArrayList list, ScoreDoc lastEmittedDoc) {
        super(request);
        this.id = id;
        this.docIds = list.buffer;
        this.size = list.size();
        this.lastEmittedDoc = lastEmittedDoc;
    }

    protected ShardFetchRequest(TransportRequest originalRequest, long id, IntArrayList list, Map<String, IntArrayList> namedList, ScoreDoc lastEmittedDoc) {
        super(originalRequest);
        this.id = id;
        this.docIds = list.buffer;
        this.size = list.size();
        this.lastEmittedDoc = lastEmittedDoc;
        if (namedList != null) {
            this.namedDocIds = new HashMap<>(namedList.size());
            for (Map.Entry<String, IntArrayList> entry : namedList.entrySet()) {
                IntArrayList docIdList = entry.getValue();
                namedDocIds.put(entry.getKey(), Arrays.copyOfRange(docIdList.buffer, 0, docIdList.size()));
            }
        }
    }

    public long id() {
        return id;
    }

    public int[] docIds() {
        return docIds;
    }

    public Map<String, int[]> namedDocIds() {
        return namedDocIds;
    }


    public int docIdsSize() {
        return size;
    }

    public ScoreDoc lastEmittedDoc() {
        return lastEmittedDoc;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
        size = in.readVInt();
        docIds = new int[size];
        for (int i = 0; i < size; i++) {
            docIds[i] = in.readVInt();
        }
        byte flag = in.readByte();
        if (flag == 1) {
            lastEmittedDoc = Lucene.readFieldDoc(in);
        } else if (flag == 2) {
            lastEmittedDoc = Lucene.readScoreDoc(in);
        } else if (flag != 0) {
            throw new IOException("Unknown flag: " + flag);
        }
        if (in.readBoolean()) {
            int namedDocIdSize = in.readVInt();
            namedDocIds = new HashMap<>(namedDocIdSize);
            for (int i = 0; i < namedDocIdSize; i++) {
                String name = in.readString();
                int docIdSize = in.readVInt();
                int[] docIds = new int[docIdSize];
                for (int j = 0; j < docIdSize; j++) {
                    docIds[j] = in.readVInt();
                }
                namedDocIds.put(name, docIds);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        out.writeVInt(size);
        for (int i = 0; i < size; i++) {
            out.writeVInt(docIds[i]);
        }
        if (lastEmittedDoc == null) {
            out.writeByte((byte) 0);
        } else if (lastEmittedDoc instanceof FieldDoc) {
            out.writeByte((byte) 1);
            Lucene.writeFieldDoc(out, (FieldDoc) lastEmittedDoc);
        } else {
            out.writeByte((byte) 2);
            Lucene.writeScoreDoc(out, lastEmittedDoc);
        }
        if (namedDocIds != null) {
            out.writeBoolean(true);
            out.writeVInt(namedDocIds.size());
            for (Map.Entry<String, int[]> entry : namedDocIds.entrySet()) {
                int[] docIds = entry.getValue();
                out.writeString(entry.getKey());
                out.writeVInt(docIds.length);
                for (int docId : docIds) {
                    out.writeVInt(docId);
                }
            }
        } else {
            out.writeBoolean(false);
        }
    }
}
