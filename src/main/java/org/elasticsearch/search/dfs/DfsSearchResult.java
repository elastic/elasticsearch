/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.dfs;

import org.apache.lucene.index.Term;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

/**
 *
 */
public class DfsSearchResult implements SearchPhaseResult {

    private static Term[] EMPTY_TERMS = new Term[0];

    private static int[] EMPTY_FREQS = new int[0];

    private SearchShardTarget shardTarget;

    private long id;

    private Term[] terms;

    private int[] freqs;

    private int maxDoc;

    public DfsSearchResult() {

    }

    public DfsSearchResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    public DfsSearchResult maxDoc(int maxDoc) {
        this.maxDoc = maxDoc;
        return this;
    }

    public int maxDoc() {
        return maxDoc;
    }

    public DfsSearchResult termsAndFreqs(Term[] terms, int[] freqs) {
        this.terms = terms;
        this.freqs = freqs;
        return this;
    }

    public Term[] terms() {
        return terms;
    }

    public int[] freqs() {
        return freqs;
    }

    public static DfsSearchResult readDfsSearchResult(StreamInput in) throws IOException, ClassNotFoundException {
        DfsSearchResult result = new DfsSearchResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readLong();
//        shardTarget = readSearchShardTarget(in);
        int termsSize = in.readVInt();
        if (termsSize == 0) {
            terms = EMPTY_TERMS;
        } else {
            terms = new Term[termsSize];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = new Term(in.readUTF(), in.readUTF());
            }
        }
        int freqsSize = in.readVInt();
        if (freqsSize == 0) {
            freqs = EMPTY_FREQS;
        } else {
            freqs = new int[freqsSize];
            for (int i = 0; i < freqs.length; i++) {
                freqs[i] = in.readVInt();
            }
        }
        maxDoc = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
//        shardTarget.writeTo(out);
        out.writeVInt(terms.length);
        for (Term term : terms) {
            out.writeUTF(term.field());
            out.writeUTF(term.text());
        }
        out.writeVInt(freqs.length);
        for (int freq : freqs) {
            out.writeVInt(freq);
        }
        out.writeVInt(maxDoc);
    }
}
