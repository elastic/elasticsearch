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

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.ScriptDocValues.Booleans;
import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class ScriptDocValuesMissingV6BehaviourTests extends ESTestCase {

    public void testZeroForMissingValueLong() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Set<String> warnings = new HashSet<>();
        Longs longs = wrap(values, (key, deprecationMessage) -> { warnings.add(deprecationMessage); });
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longs.setNextDocId(d);
            assertEquals(0, longs.getValue());
        }
        assertThat(warnings, contains(equalTo(
            "returning default values for missing document values is deprecated. " +
            "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
            "to make behaviour compatible with future major versions!")));
    }

    public void testEpochForMissingValueDate() throws IOException {
        final JodaCompatibleZonedDateTime EPOCH = new JodaCompatibleZonedDateTime(Instant.EPOCH, ZoneOffset.UTC);
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Set<String> warnings = new HashSet<>();
        Dates dates = wrapDates(values, (key, deprecationMessage) -> { warnings.add(deprecationMessage); });
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            dates.setNextDocId(d);
            assertEquals(EPOCH, dates.getValue());
        }
        assertThat(warnings, contains(equalTo(
            "returning default values for missing document values is deprecated. " +
            "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
            "to make behaviour compatible with future major versions!")));
    }

    public void testFalseForMissingValueBoolean() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Set<String> warnings = new HashSet<>();
        Booleans bools = wrapBooleans(values, (key, deprecationMessage) -> { warnings.add(deprecationMessage); });
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            bools.setNextDocId(d);
            assertEquals(false, bools.getValue());
        }
        logger.error("ASDFAFD {}", warnings);
        assertThat(warnings, contains(equalTo(
            "returning default values for missing document values is deprecated. " +
            "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
            "to make behaviour compatible with future major versions!")));
    }

    public void testNullForMissingValueGeo() throws IOException{
        Set<String> warnings = new HashSet<>();
        final MultiGeoPointValues values = wrap(new GeoPoint[0]);
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(
            values, (key, deprecationMessage) -> { warnings.add(deprecationMessage); });
        script.setNextDocId(0);
        assertEquals(null, script.getValue());
        assertThat(warnings, contains(equalTo(
            "returning default values for missing document values is deprecated. " +
            "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
            "to make behaviour compatible with future major versions!")));
    }


    private Longs wrap(long[][] values, BiConsumer<String, String> deprecationCallback) {
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
        }, deprecationCallback);
    }

    private Booleans wrapBooleans(long[][] values, BiConsumer<String, String> deprecationCallback) {
        return new Booleans(new AbstractSortedNumericDocValues() {
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
        }, deprecationCallback);
    }

    private Dates wrapDates(long[][] values, BiConsumer<String, String> deprecationCallback) {
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
        }, deprecationCallback);
    }


   private static MultiGeoPointValues wrap(final GeoPoint... points) {
        return new MultiGeoPointValues() {
            int docID = -1;
            int i;
            @Override
            public GeoPoint nextValue() {
                if (docID != 0) {
                    fail();
                }
                return points[i++];
            }
            @Override
            public boolean advanceExact(int docId) {
                docID = docId;
                return points.length > 0;
            }
            @Override
            public int docValueCount() {
                if (docID != 0) {
                    return 0;
                }
                return points.length;
            }
        };
    }

}
