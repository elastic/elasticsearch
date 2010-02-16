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

import com.google.common.collect.Iterators;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.trove.ExtTObjectIntHasMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.action.terms.TermFreq.*;

/**
 * @author kimchy (Shay Banon)
 */
public class FieldTermsFreq implements Streamable, Iterable<TermFreq> {

    private String fieldName;

    private TermFreq[] termsFreqs;

    private transient ExtTObjectIntHasMap<String> termsFreqMap;

    private FieldTermsFreq() {

    }

    public FieldTermsFreq(String fieldName, TermFreq[] termsFreqs) {
        this.fieldName = fieldName;
        this.termsFreqs = termsFreqs;
    }

    public String fieldName() {
        return this.fieldName;
    }

    public TermFreq[] termsFreqs() {
        return this.termsFreqs;
    }

    /**
     * Returns the document frequency of a term, <tt>-1</tt> if the term does not exists.
     */
    public int docFreq(String term) {
        if (termsFreqMap == null) {
            ExtTObjectIntHasMap<String> termsFreqMap = new ExtTObjectIntHasMap<String>().defaultReturnValue(-1);
            for (TermFreq termFreq : termsFreqs) {
                termsFreqMap.put(termFreq.term(), termFreq.docFreq());
            }
            this.termsFreqMap = termsFreqMap;
        }
        return termsFreqMap.get(term);
    }

    @Override public Iterator<TermFreq> iterator() {
        return Iterators.forArray(termsFreqs);
    }

    public static FieldTermsFreq readFieldTermsFreq(DataInput in) throws IOException, ClassNotFoundException {
        FieldTermsFreq fieldTermsFreq = new FieldTermsFreq();
        fieldTermsFreq.readFrom(in);
        return fieldTermsFreq;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        fieldName = in.readUTF();
        termsFreqs = new TermFreq[in.readInt()];
        for (int i = 0; i < termsFreqs.length; i++) {
            termsFreqs[i] = readTermFreq(in);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(fieldName);
        out.writeInt(termsFreqs.length);
        for (TermFreq termFreq : termsFreqs) {
            termFreq.writeTo(out);
        }
    }
}
