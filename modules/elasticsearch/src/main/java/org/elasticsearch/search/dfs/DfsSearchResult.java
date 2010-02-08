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

package org.elasticsearch.search.dfs;

import org.apache.lucene.index.Term;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.search.SearchShardTarget.*;

/**
 * @author kimchy (Shay Banon)
 */
public class DfsSearchResult implements Streamable {

    private static Term[] EMPTY_TERMS = new Term[0];

    private static int[] EMPTY_FREQS = new int[0];

    private SearchShardTarget shardTarget;

    private long id;

    private Term[] terms;

    private int[] freqs;

    private int numDocs;

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

    public DfsSearchResult numDocs(int numDocs) {
        this.numDocs = numDocs;
        return this;
    }

    public int numDocs() {
        return numDocs;
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

    public static DfsSearchResult readDfsSearchResult(DataInput in) throws IOException, ClassNotFoundException {
        DfsSearchResult result = new DfsSearchResult();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        shardTarget = readSearchShardTarget(in);
        int termsSize = in.readInt();
        if (termsSize == 0) {
            terms = EMPTY_TERMS;
        } else {
            terms = new Term[termsSize];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = new Term(in.readUTF(), in.readUTF());
            }
        }
        int freqsSize = in.readInt();
        if (freqsSize == 0) {
            freqs = EMPTY_FREQS;
        } else {
            freqs = new int[freqsSize];
            for (int i = 0; i < freqs.length; i++) {
                freqs[i] = in.readInt();
            }
        }
        numDocs = in.readInt();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeLong(id);
        shardTarget.writeTo(out);
        out.writeInt(terms.length);
        for (Term term : terms) {
            out.writeUTF(term.field());
            out.writeUTF(term.text());
        }
        out.writeInt(freqs.length);
        for (int freq : freqs) {
            out.writeInt(freq);
        }
        out.writeInt(numDocs);
    }
}
