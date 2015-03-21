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
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.type.ParsedScrollId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Shard level fetch base request. Holds all the info needed to execute a fetch.
 * Used with search scroll as the original request doesn't hold indices.
 */
public class ShardFetchRequest extends TransportRequest {

    private long id;

    private int[] docIds;

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

    protected ShardFetchRequest(TransportRequest originalRequest, long id, IntArrayList list, ScoreDoc lastEmittedDoc) {
        super(originalRequest);
        this.id = id;
        this.docIds = list.buffer;
        this.size = list.size();
        this.lastEmittedDoc = lastEmittedDoc;
    }

    public long id() {
        return id;
    }

    public int[] docIds() {
        return docIds;
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
    }
}
