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
import org.elasticsearch.util.gnu.trove.TObjectIntProcedure;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.trove.ExtTObjectIntHasMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class AggregatedDfs implements Streamable {

    private ExtTObjectIntHasMap<Term> dfMap;

    private int numDocs;

    private AggregatedDfs() {

    }

    public AggregatedDfs(ExtTObjectIntHasMap<Term> dfMap, int numDocs) {
        this.dfMap = dfMap.defaultReturnValue(-1);
        this.numDocs = numDocs;
    }

    public ExtTObjectIntHasMap<Term> dfMap() {
        return dfMap;
    }

    public int numDocs() {
        return numDocs;
    }

    public static AggregatedDfs readAggregatedDfs(DataInput in) throws IOException, ClassNotFoundException {
        AggregatedDfs result = new AggregatedDfs();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        dfMap = new ExtTObjectIntHasMap<Term>(size).defaultReturnValue(-1);
        for (int i = 0; i < size; i++) {
            dfMap.put(new Term(in.readUTF(), in.readUTF()), in.readInt());
        }
        numDocs = in.readInt();
    }

    @Override public void writeTo(final DataOutput out) throws IOException {
        out.writeInt(dfMap.size());
        WriteToProcedure writeToProcedure = new WriteToProcedure(out);
        if (!dfMap.forEachEntry(writeToProcedure)) {
            throw writeToProcedure.exception;
        }
        out.writeInt(numDocs);
    }

    private static class WriteToProcedure implements TObjectIntProcedure<Term> {

        private final DataOutput out;

        IOException exception;

        private WriteToProcedure(DataOutput out) {
            this.out = out;
        }

        @Override public boolean execute(Term a, int b) {
            try {
                out.writeUTF(a.field());
                out.writeUTF(a.text());
                out.writeInt(b);
                return true;
            } catch (IOException e) {
                exception = e;
            }
            return false;
        }
    }
}
