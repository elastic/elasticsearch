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

import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;


public class ScriptDocValuesDatesTests extends ESTestCase {
    public void test() throws IOException {
        assertDateDocValues(millis -> ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC")));
    }

    public void assertDateDocValues(Function<Long, Object> datetimeCtor) throws IOException {
        long[][] values = new long[between(3, 10)][];
        Object[][] expectedDates = new Object[values.length][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            expectedDates[d] = new Object[values[d].length];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomNonNegativeLong();
                expectedDates[d][i] = datetimeCtor.apply(values[d][i]);
            }
        }
        Dates dates = wrap(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            dates.setNextDocId(d);
            if (expectedDates[d].length > 0) {
                assertEquals(expectedDates[d][0] , dates.getValue());
            } else {
                Exception e = expectThrows(IllegalStateException.class, () -> dates.getValue());
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
            }

            assertEquals(values[d].length, dates.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(expectedDates[d][i], dates.get(i));
            }
        }
    }

    private Dates wrap(long[][] values) {
        return new Dates(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;

            @Override
            public boolean advanceExact(int doc) {
                current = values[doc];
                i = 0;
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
