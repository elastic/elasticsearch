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

package org.elasticsearch.index.field.data.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.trove.TIntArrayList;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataOptions;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author kimchy (shay.banon)
 */
public class FieldDataLoader {

    @SuppressWarnings({"StringEquality"})
    public static <T extends FieldData> T load(IndexReader reader, String field, FieldDataOptions options, TypeLoader<T> loader) throws IOException {

        loader.init(options);

        field = StringHelper.intern(field);
        int[][] orders = new int[reader.maxDoc()][];

        int t = 1;  // current term number

        boolean multiValued = false;
        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms(new Term(field));
        try {
            do {
                Term term = termEnum.term();
                if (term == null || term.field() != field) break;
                loader.collectTerm(term.text());
                termDocs.seek(termEnum);
                int df = 0;
                while (termDocs.next()) {
                    df++;
                    int doc = termDocs.doc();
                    int[] orderPerDoc = orders[doc];
                    if (orderPerDoc == null) {
                        orderPerDoc = new int[1];
                        orderPerDoc[0] = t;
                        orders[doc] = orderPerDoc;
                    } else {
                        multiValued = true;
                        orderPerDoc = Arrays.copyOf(orderPerDoc, orderPerDoc.length + 1);
                        orderPerDoc[orderPerDoc.length - 1] = t;
                        orders[doc] = orderPerDoc;
                    }
                }
                if (options.hasFreqs()) {
                    loader.collectFreq(df);
                }

                t++;
            } while (termEnum.next());
        } catch (RuntimeException e) {
            if (e.getClass().getName().endsWith("StopFillCacheException")) {
                // all is well, in case numeric parsers are used.
            } else {
                throw e;
            }
        } finally {
            termDocs.close();
            termEnum.close();
        }

        if (multiValued) {
            return loader.buildMultiValue(field, orders);
        } else {
            // optimize for a single valued
            int[] sOrders = new int[reader.maxDoc()];
            for (int i = 0; i < orders.length; i++) {
                if (orders[i] != null) {
                    sOrders[i] = orders[i][0];
                }
            }
            return loader.buildSingleValue(field, sOrders);
        }
    }

    public static interface TypeLoader<T extends FieldData> {

        void init(FieldDataOptions options);

        void collectTerm(String term);

        void collectFreq(int freq);

        T buildSingleValue(String fieldName, int[] order);

        T buildMultiValue(String fieldName, int[][] order);
    }

    public static abstract class FreqsTypeLoader<T extends FieldData> implements TypeLoader<T> {

        protected FieldDataOptions options;

        private TIntArrayList freqs;

        protected FreqsTypeLoader() {
        }

        @Override public void init(FieldDataOptions options) {
            this.options = options;
            if (options.hasFreqs()) {
                freqs = new TIntArrayList();
                freqs.add(0);
            }
        }

        @Override public void collectFreq(int freq) {
            freqs.add(freq);
        }

        protected int[] buildFreqs() {
            if (freqs == null) {
                return null;
            }
            return freqs.toNativeArray();
        }
    }
}
