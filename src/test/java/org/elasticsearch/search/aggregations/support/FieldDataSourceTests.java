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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;
public class FieldDataSourceTests extends ElasticsearchTestCase {

    private static BytesValues randomBytesValues() {
        final boolean multiValued = randomBoolean();
        final int maxLength = rarely() ? 3 : 10;
        return new BytesValues(multiValued) {
            private final BytesRef scratch = new BytesRef();
            BytesRef previous;
            @Override
            public int setDocument(int docId) {
                return randomInt(multiValued ? 10 : 1);
            }
            @Override
            public BytesRef nextValue() {
                if (previous != null && randomBoolean()) {
                    scratch.copyBytes(previous);
                } else {
                    scratch.copyChars(randomAsciiOfLength(maxLength));
                }
                previous = BytesRef.deepCopyOf(scratch);
                return scratch;
            }

        };
    }

    private static SearchScript randomScript() {
        return new SearchScript() {

            @Override
            public void setNextVar(String name, Object value) {
            }

            @Override
            public Object run() {
                return randomAsciiOfLength(5);
            }

            @Override
            public Object unwrap(Object value) {
                return value;
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
            }

            @Override
            public void setScorer(Scorer scorer) {
            }

            @Override
            public void setNextDocId(int doc) {
            }

            @Override
            public void setNextSource(Map<String, Object> source) {
            }

            @Override
            public void setNextScore(float score) {
            }

            @Override
            public float runAsFloat() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long runAsLong() {
                throw new UnsupportedOperationException();
            }

            @Override
            public double runAsDouble() {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Test
    public void sortedUniqueBytesValues() {
        assertSortedAndUnique(new ValuesSource.Bytes.SortedAndUnique.SortedUniqueBytesValues(randomBytesValues()));
    }

    private static void assertSortedAndUnique(BytesValues values) {
        final int numDocs = scaledRandomIntBetween(10, 100);
        ArrayList<BytesRef> ref = new ArrayList<BytesRef>();
        for (int i = 0; i < numDocs; ++i) {
            final int valueCount = values.setDocument(i);
            ref.clear();
            for (int j = 0; j < valueCount; ++j) {
                final BytesRef term = values.nextValue();
                if (j > 0) {
                    assertThat(BytesRef.getUTF8SortedAsUnicodeComparator().compare(ref.get(ref.size() - 1), term), lessThan(0));
                }
                ref.add(BytesRef.deepCopyOf(term));
            }
        }
    }

}
