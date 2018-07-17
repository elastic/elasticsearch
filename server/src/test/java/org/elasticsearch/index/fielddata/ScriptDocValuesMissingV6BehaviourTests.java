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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.index.fielddata.ScriptDocValues.Booleans;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.test.ESTestCase;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.singletonList;

public class ScriptDocValuesMissingV6BehaviourTests extends ESTestCase {

    public void testScriptMissingValuesWarning(){
        new ScriptModule(Settings.EMPTY, singletonList(new ScriptPlugin() {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap("1", script -> "1"));
            }
        }));
        assertWarnings("Script: returning default values for missing document values is deprecated. " +
            "Set system property '-Des.scripting.exception_for_missing_value=true' " +
            "to make behaviour compatible with future major versions.");
    }

    public void testZeroForMissingValueLong() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Longs longs = wrap(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longs.setNextDocId(d);
            assertEquals(0, longs.getValue());
        }
    }

    public void testEpochForMissingValueDate() throws IOException {
        final ReadableDateTime EPOCH = new DateTime(0, DateTimeZone.UTC);
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Dates dates = wrapDates(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            dates.setNextDocId(d);
            assertEquals(EPOCH, dates.getValue());
        }
    }

    public void testFalseForMissingValueBoolean() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Booleans bools = wrapBooleans(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            bools.setNextDocId(d);
            assertEquals(false, bools.getValue());
        }
    }

    public void testNullForMissingValueGeo() throws IOException{
        final MultiGeoPointValues values = wrap(new GeoPoint[0]);
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(values);
        script.setNextDocId(0);
        assertEquals(null, script.getValue());
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

    private Booleans wrapBooleans(long[][] values) {
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
        });
    }

    private Dates wrapDates(long[][] values) {
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
