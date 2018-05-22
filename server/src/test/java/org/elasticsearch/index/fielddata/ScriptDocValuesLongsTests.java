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

package org.elasticsearch.index.fielddata;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class ScriptDocValuesLongsTests extends ESTestCase {
    public void testLongs() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomLong();
            }
        }
        Longs longs = wrap(values);

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longs.setNextDocId(d);
            assertEquals(values[d].length > 0 ? values[d][0] : 0, longs.getValue());

            assertEquals(values[d].length, longs.size());
            assertEquals(values[d].length, longs.getValues().size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], longs.get(i).longValue());
                assertEquals(values[d][i], longs.getValues().get(i).longValue());
            }

            Exception e = expectThrows(UnsupportedOperationException.class, () -> longs.getValues().add(100L));
            assertEquals("doc values are unmodifiable", e.getMessage());
        }
    }

    public void testLongsMinMaxSumAvg() throws IOException {
        long[][] values = new long[between(3, 10)][];
        long[] mins = new long[values.length];
        long[] maxs = new long[values.length];
        long[] sums = new long[values.length];
        double[] avgs = new double[values.length];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            mins[d] = values[d].length > 0 ? Long.MAX_VALUE : 0L;
            maxs[d] = values[d].length > 0 ? Long.MIN_VALUE : 0L;
            sums[d] = 0L;
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomLong();
                mins[d] = mins[d] > values[d][i] ? values[d][i] : mins[d];
                maxs[d] = maxs[d] < values[d][i] ? values[d][i] : maxs[d];
                sums[d] += values[d][i];
            }
            avgs[d] = values[d].length > 0 ? sums[d] * 1.0/ values[d].length  : 0L;
            Arrays.sort(values[d]);
        }
        Longs longs = wrap(values);

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longs.setNextDocId(d);
            assertEquals(mins[d], longs.getMin());
            assertEquals(maxs[d], longs.getMax());
            assertEquals(sums[d], longs.getSum());
            assertEquals(avgs[d], longs.getAvg(), 0.0);
        }
    }


    private Longs wrap(long[][] values) {
        return new Longs(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;

            @Override
            public boolean advanceExact(int doc) {
                i = 0;
                current = values[doc];
                return current.length > 0;
            }
            @Override
            public int docValueCount() {
                return current.length;
            }
            @Override
            public long nextValue() {
                return current[i++];
            }
        });
    }
}
