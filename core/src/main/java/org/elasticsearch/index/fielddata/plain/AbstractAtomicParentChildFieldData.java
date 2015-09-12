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

package org.elasticsearch.index.fielddata.plain;

import com.google.common.collect.ImmutableSet;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AtomicParentChildFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;


/**
 */
abstract class AbstractAtomicParentChildFieldData implements AtomicParentChildFieldData {

    @Override
    public final ScriptDocValues getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return new SortedBinaryDocValues() {

            private final BytesRef[] terms = new BytesRef[2];
            private int count;

            @Override
            public void setDocument(int docId) {
                count = 0;
                for (String type : types()) {
                    final SortedDocValues values = getOrdinalsValues(type);
                    final int ord = values.getOrd(docId);
                    if (ord >= 0) {
                        terms[count++] = values.lookupOrd(ord);
                    }
                }
                assert count <= 2 : "A single doc can potentially be both parent and child, so the maximum allowed values is 2";
                if (count > 1) {
                    int cmp = terms[0].compareTo(terms[1]);
                    if (cmp > 0) {
                        ArrayUtil.swap(terms, 0, 1);
                    } else if (cmp == 0) {
                        // If the id is the same between types the only omit one. For example: a doc has parent#1 in _uid field and has grand_parent#1 in _parent field.
                        count = 1;
                    }
                }
            }

            @Override
            public int count() {
                return count;
            }

            @Override
            public BytesRef valueAt(int index) {
                return terms[index];
            }
        };
    }

    public static AtomicParentChildFieldData empty() {
        return new AbstractAtomicParentChildFieldData() {

            @Override
            public long ramBytesUsed() {
                return 0;
            }
            
            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public SortedDocValues getOrdinalsValues(String type) {
                return DocValues.emptySorted();
            }

            @Override
            public Set<String> types() {
                return ImmutableSet.of();
            }
        };
    }
}
