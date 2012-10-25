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

package org.elasticsearch.index.field.data.support;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.field.data.FieldData;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class FieldDataLoader {

    @SuppressWarnings({"StringEquality"})
    public static <T extends FieldData> T load(AtomicReader reader, String field, TypeLoader<T> loader) throws IOException {

        loader.init();
        ArrayList<int[]> ordinals = new ArrayList<int[]>();
        int[] idx = new int[reader.maxDoc()];
        ordinals.add(new int[reader.maxDoc()]);

        int t = 1;  // current term number

        Terms terms = reader.terms(field);
        if (terms == null) {
            return  loader.buildSingleValue(field, new int[0]); // Return empty field data if field doesn't exists.
        }

        TermsEnum termsEnum = terms.iterator(null);
        try {
            DocsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.term()) {
                loader.collectTerm(BytesRef.deepCopyOf(term));
                docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, 0);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    int[] ordinal;
                    if (idx[docId] >= ordinals.size()) {
                        ordinal = new int[reader.maxDoc()];
                        ordinals.add(ordinal);
                    } else {
                        ordinal = ordinals.get(idx[docId]);
                    }
                    ordinal[docId] = t;
                    idx[docId]++;
                }
            }
        } catch (RuntimeException e) {
            if (e.getClass().getName().endsWith("StopFillCacheException")) {
                // all is well, in case numeric parsers are used.
            } else {
                throw e;
            }
        }

        if (ordinals.size() == 1) {
            return loader.buildSingleValue(field, ordinals.get(0));
        } else {
            int[][] nativeOrdinals = new int[ordinals.size()][];
            for (int i = 0; i < nativeOrdinals.length; i++) {
                nativeOrdinals[i] = ordinals.get(i);
            }
            return loader.buildMultiValue(field, nativeOrdinals);
        }
    }

    public static interface TypeLoader<T extends FieldData> {

        void init();

        void collectTerm(BytesRef term);

        T buildSingleValue(String fieldName, int[] ordinals);

        T buildMultiValue(String fieldName, int[][] ordinals);
    }

    public static abstract class FreqsTypeLoader<T extends FieldData> implements TypeLoader<T> {

        protected FreqsTypeLoader() {
        }

        @Override
        public void init() {
        }
    }
}
