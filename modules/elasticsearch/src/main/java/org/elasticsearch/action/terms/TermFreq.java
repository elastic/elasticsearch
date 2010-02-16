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

import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * A tuple of term and its document frequency (in how many documents this term exists).
 *
 * @author kimchy (Shay Banon)
 */
public class TermFreq implements Streamable {

    /**
     * A frequency based comparator with higher frequencies first.
     */
    private static final Comparator<TermFreq> freqComparator = new Comparator<TermFreq>() {
        @Override public int compare(TermFreq o1, TermFreq o2) {
            int i = o2.docFreq() - o1.docFreq();
            if (i == 0) {
                i = o1.term().compareTo(o2.term());
            }
            return i;
        }
    };

    /**
     * Lexical based comparator.
     */
    private static final Comparator<TermFreq> termComparator = new Comparator<TermFreq>() {
        @Override public int compare(TermFreq o1, TermFreq o2) {
            int i = o1.term().compareTo(o2.term());
            if (i == 0) {
                i = o1.docFreq() - o2.docFreq();
            }
            return i;
        }
    };

    /**
     * A frequency based comparator with higher frequencies first.
     */
    public static Comparator<TermFreq> freqComparator() {
        return freqComparator;
    }

    /**
     * Lexical based comparator.
     */
    public static Comparator<TermFreq> termComparator() {
        return termComparator;
    }

    private String term;

    private int docFreq;

    private TermFreq() {

    }

    TermFreq(String term, int docFreq) {
        this.term = term;
        this.docFreq = docFreq;
    }

    /**
     * The term.
     */
    public String term() {
        return term;
    }

    /**
     * The document frequency of the term (in how many documents this term exists).
     */
    public int docFreq() {
        return docFreq;
    }

    public static TermFreq readTermFreq(DataInput in) throws IOException, ClassNotFoundException {
        TermFreq termFreq = new TermFreq();
        termFreq.readFrom(in);
        return termFreq;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        term = in.readUTF();
        docFreq = in.readInt();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(term);
        out.writeInt(docFreq);
    }
}
