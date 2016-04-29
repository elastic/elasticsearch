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

package org.elasticsearch.fieldstats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.fieldstats.IndexConstraint;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.fieldstats.IndexConstraint.Comparison.GTE;
import static org.elasticsearch.action.fieldstats.IndexConstraint.Comparison.LT;
import static org.elasticsearch.action.fieldstats.IndexConstraint.Comparison.LTE;
import static org.elasticsearch.action.fieldstats.IndexConstraint.Property.MAX;
import static org.elasticsearch.action.fieldstats.IndexConstraint.Property.MIN;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class FieldStatsIntegrationIT extends ESIntegTestCase {

    public void testRandom() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                    "test",
                    "string", "type=text",
                    "date", "type=date",
                    "double", "type=double",
                    "float", "type=float",
                    "long", "type=long",
                    "integer", "type=integer",
                    "short", "type=short",
                    "byte", "type=byte"));
        ensureGreen("test");

        // index=false
        assertAcked(prepareCreate("test1").addMapping(
            "test",
            "string", "type=text,index=false",
            "date", "type=date,index=false",
            "double", "type=double,index=false",
            "float", "type=float,index=false",
            "long", "type=long,index=false",
            "integer", "type=integer,index=false",
            "short", "type=short,index=false",
            "byte", "type=byte,index=false"
        ));
        ensureGreen("test1");

        // no value indexed
        assertAcked(prepareCreate("test3").addMapping(
            "test",
            "string", "type=text,index=false",
            "date", "type=date,index=false",
            "double", "type=double,index=false",
            "float", "type=float,index=false",
            "long", "type=long,index=false",
            "integer", "type=integer,index=false",
            "short", "type=short,index=false",
            "byte", "type=byte,index=false"
        ));
        ensureGreen("test3");

        long minByte = Byte.MAX_VALUE;
        long maxByte = Byte.MIN_VALUE;
        long minShort = Short.MAX_VALUE;
        long maxShort = Short.MIN_VALUE;
        long minInt = Integer.MAX_VALUE;
        long maxInt = Integer.MIN_VALUE;
        long minLong = Long.MAX_VALUE;
        long maxLong = Long.MIN_VALUE;
        double minFloat = Float.MAX_VALUE;
        double maxFloat = Float.MIN_VALUE;
        double minDouble = Double.MAX_VALUE;
        double maxDouble = Double.MIN_VALUE;
        String minString = new String(Character.toChars(1114111));
        String maxString = "0";

        int numDocs = scaledRandomIntBetween(128, 1024);
        List<IndexRequestBuilder> request = new ArrayList<>(numDocs);
        for (int doc = 0; doc < numDocs; doc++) {
            byte b = randomByte();
            minByte = Math.min(minByte, b);
            maxByte = Math.max(maxByte, b);
            short s = randomShort();
            minShort = Math.min(minShort, s);
            maxShort = Math.max(maxShort, s);
            int i = randomInt();
            minInt = Math.min(minInt, i);
            maxInt = Math.max(maxInt, i);
            long l = randomLong();
            minLong = Math.min(minLong, l);
            maxLong = Math.max(maxLong, l);
            float f = randomFloat();
            minFloat = Math.min(minFloat, f);
            maxFloat = Math.max(maxFloat, f);
            double d = randomDouble();
            minDouble = Math.min(minDouble, d);
            maxDouble = Math.max(maxDouble, d);
            String str = randomRealisticUnicodeOfLength(3);
            if (str.compareTo(minString) < 0) {
                minString = str;
            }
            if (str.compareTo(maxString) > 0) {
                maxString = str;
            }

            request.add(client().prepareIndex("test", "test", Integer.toString(doc))
                            .setSource("byte", b,
                                "short", s,
                                "integer", i,
                                "long", l,
                                "float", f,
                                "double", d,
                                "string", str)
            );
        }
        indexRandom(true, false, request);

        FieldStatsResponse response = client()
            .prepareFieldStats()
            .setFields("byte", "short", "integer", "long", "float", "double", "string").get();
        assertAllSuccessful(response);

        for (FieldStats stats : response.getAllFieldStats().values()) {
            assertThat(stats.getMaxDoc(), equalTo((long) numDocs));
            assertThat(stats.getDocCount(), equalTo((long) numDocs));
            assertThat(stats.getDensity(), equalTo(100));
        }

        assertThat(response.getAllFieldStats().get("byte").getMinValue(), equalTo(minByte));
        assertThat(response.getAllFieldStats().get("byte").getMaxValue(), equalTo(maxByte));
        assertThat(response.getAllFieldStats().get("short").getMinValue(), equalTo(minShort));
        assertThat(response.getAllFieldStats().get("short").getMaxValue(), equalTo(maxShort));
        assertThat(response.getAllFieldStats().get("integer").getMinValue(), equalTo(minInt));
        assertThat(response.getAllFieldStats().get("integer").getMaxValue(), equalTo(maxInt));
        assertThat(response.getAllFieldStats().get("long").getMinValue(), equalTo(minLong));
        assertThat(response.getAllFieldStats().get("long").getMaxValue(), equalTo(maxLong));
        assertThat(response.getAllFieldStats().get("float").getMinValue(), equalTo(minFloat));
        assertThat(response.getAllFieldStats().get("float").getMaxValue(), equalTo(maxFloat));
        assertThat(response.getAllFieldStats().get("double").getMinValue(), equalTo(minDouble));
        assertThat(response.getAllFieldStats().get("double").getMaxValue(), equalTo(maxDouble));
    }

    public void testFieldStatsIndexLevel() throws Exception {
        assertAcked(prepareCreate("test1").addMapping(
                "test", "value", "type=long"
        ));
        assertAcked(prepareCreate("test2").addMapping(
                "test", "value", "type=long"
        ));
        assertAcked(prepareCreate("test3").addMapping(
                "test", "value", "type=long"
        ));
        ensureGreen("test1", "test2", "test3");

        indexRange("test1", -10, 100);
        indexRange("test2", 101, 200);
        indexRange("test3", 201, 300);

        // default:
        FieldStatsResponse response = client().prepareFieldStats().setFields("value").get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats().get("value").getMinValue(), equalTo(-10L));
        assertThat(response.getAllFieldStats().get("value").getMaxValue(), equalTo(300L));
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("_all").get("value").getMinValue(), equalTo(-10L));
        assertThat(response.getIndicesMergedFieldStats().get("_all").get("value").getMaxValue(), equalTo(300L));

        // Level: cluster
        response = client().prepareFieldStats().setFields("value").setLevel("cluster").get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats().get("value").getMinValue(), equalTo(-10L));
        assertThat(response.getAllFieldStats().get("value").getMaxValue(), equalTo(300L));
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("_all").get("value").getMinValue(), equalTo(-10L));
        assertThat(response.getIndicesMergedFieldStats().get("_all").get("value").getMaxValue(), equalTo(300L));

        // Level: indices
        response = client().prepareFieldStats().setFields("value").setLevel("indices").get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(3));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), equalTo(-10L));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMaxValue(), equalTo(100L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), equalTo(101L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMaxValue(), equalTo(200L));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMinValue(), equalTo(201L));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMaxValue(), equalTo(300L));

        // Illegal level option:
        try {
            client().prepareFieldStats().setFields("value").setLevel("illegal").get();
            fail();
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), equalTo("Validation Failed: 1: invalid level option [illegal];"));
        }
    }

    public void testIncompatibleFieldTypesSingleField() {
        assertAcked(prepareCreate("test1").addMapping(
            "test", "value", "type=long"
        ));
        assertAcked(prepareCreate("test2").addMapping(
            "test", "value", "type=text"
        ));
        ensureGreen("test1", "test2");

        client().prepareIndex("test1", "test").setSource("value", 1L).get();
        client().prepareIndex("test1", "test").setSource("value", 2L).get();
        client().prepareIndex("test2", "test").setSource("value", "a").get();
        client().prepareIndex("test2", "test").setSource("value", "b").get();
        refresh();

        FieldStatsResponse response = client().prepareFieldStats().setFields("value", "value2").get();
        assertAllSuccessful(response);
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("_all").size(), equalTo(0));
        assertThat(response.getConflicts().size(), equalTo(1));
        assertThat(response.getConflicts().get("value"),
            equalTo("Field [value] of type [text] conflicts with existing field of type [whole-number] " +
                "in other index."));

        response = client().prepareFieldStats().setFields("value").setLevel("indices").get();
        assertAllSuccessful(response);
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(2));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), equalTo(1L));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMaxValue(), equalTo(2L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
            equalTo(new BytesRef("a")));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMaxValue(),
            equalTo(new BytesRef("b")));
    }

    @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-7257")
    public void testIncompatibleFieldTypesMultipleFields() {
        assertAcked(prepareCreate("test1").addMapping(
                "test", "value", "type=long", "value2", "type=long"
        ));
        assertAcked(prepareCreate("test2").addMapping(
                "test", "value", "type=text", "value2", "type=long"
        ));
        ensureGreen("test1", "test2");

        client().prepareIndex("test1", "test").setSource("value", 1L, "value2", 1L).get();
        client().prepareIndex("test1", "test").setSource("value", 2L).get();
        client().prepareIndex("test2", "test").setSource("value", "a").get();
        client().prepareIndex("test2", "test").setSource("value", "b").get();
        refresh();

        FieldStatsResponse response = client().prepareFieldStats().setFields("value", "value2").get();
        assertAllSuccessful(response);
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("_all").size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("_all").get("value2").getMinValue(), equalTo(1L));
        assertThat(response.getIndicesMergedFieldStats().get("_all").get("value2").getMaxValue(), equalTo(1L));
        assertThat(response.getConflicts().size(), equalTo(1));
        assertThat(response.getConflicts().get("value"),
            equalTo("Field [value] of type [text] conflicts with existing field of type [whole-number] " +
                "in other index."));

        response = client().prepareFieldStats().setFields("value", "value2").setLevel("indices").get();
        assertAllSuccessful(response);
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(2));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), equalTo(1L));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMaxValue(), equalTo(2L));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value2").getMinValue(), equalTo(1L));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value2").getMaxValue(), equalTo(1L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
            equalTo(new BytesRef("a")));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMaxValue(),
            equalTo(new BytesRef("b")));
    }

    public void testFieldStatsFiltering() throws Exception {
        assertAcked(prepareCreate("test1").addMapping(
                "test", "value", "type=long"
        ));
        assertAcked(prepareCreate("test2").addMapping(
                "test", "value", "type=long"
        ));
        assertAcked(prepareCreate("test3").addMapping(
                "test", "value", "type=long"
        ));
        ensureGreen("test1", "test2", "test3");

        indexRange("test1", -10, 100);
        indexRange("test2", 101, 200);
        indexRange("test3", 201, 300);

        FieldStatsResponse response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MIN, GTE, "200"),
                    new IndexConstraint("value", MAX , LTE, "300"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMinValue(), equalTo(201L));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMaxValue(), equalTo(300L));

        response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MAX, LTE, "200"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(2));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), equalTo(-10L));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("value").getMaxValue(), equalTo(100L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), equalTo(101L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMaxValue(), equalTo(200L));

        response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MIN, GTE, "100"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(2));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), equalTo(101L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMaxValue(), equalTo(200L));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMinValue(), equalTo(201L));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMaxValue(), equalTo(300L));

        response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MIN, GTE, "-20"),
                    new IndexConstraint("value", MAX, LT, "-10"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(0));

        response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MIN, GTE, "-100"),
                    new IndexConstraint("value", MAX, LTE, "-20"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(0));

        response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MIN, GTE, "100"),
                    new IndexConstraint("value", MAX, LTE, "200"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), equalTo(101L));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("value").getMaxValue(), equalTo(200L));

        response = client().prepareFieldStats()
                .setFields("value")
                .setIndexContraints(new IndexConstraint("value", MIN, GTE, "150"),
                    new IndexConstraint("value", MAX, LTE, "300"))
                .setLevel("indices")
                .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMinValue(), equalTo(201L));
        assertThat(response.getIndicesMergedFieldStats().get("test3").get("value").getMaxValue(), equalTo(300L));
    }

    public void testIncompatibleFilter() throws Exception {
        assertAcked(prepareCreate("test1").addMapping(
                "test", "value", "type=long"
        ));
        indexRange("test1", -10, 100);
        try {
            client().prepareFieldStats()
                    .setFields("value")
                    .setIndexContraints(new IndexConstraint("value", MAX, LTE, "abc"))
                    .setLevel("indices")
                    .get();
            fail("exception should have been thrown, because value abc is incompatible");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("For input string: \"abc\""));
        }
    }

    public void testWildcardFields() throws Exception {
        assertAcked(prepareCreate("test1").addMapping(
            "test", "foo", "type=long", "foobar", "type=text", "barfoo", "type=long"
        ));
        assertAcked(prepareCreate("test2").addMapping(
            "test", "foobar", "type=text", "barfoo", "type=long"
        ));
        ensureGreen("test1", "test2");
        FieldStatsResponse response = client().prepareFieldStats()
            .setFields("foo*")
            .get();
        assertAllSuccessful(response);
        assertThat(response.getAllFieldStats().size(), equalTo(2));
        assertThat(response.getAllFieldStats().get("foo").getMinValue(), nullValue());
        assertThat(response.getAllFieldStats().get("foobar").getMaxValue(), nullValue());

        response = client().prepareFieldStats()
            .setFields("foo*")
            .setLevel("indices")
            .get();
        assertAllSuccessful(response);
        assertThat(response.getIndicesMergedFieldStats().size(), equalTo(2));
        assertThat(response.getIndicesMergedFieldStats().get("test1").size(), equalTo(2));
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("foo").getMinValue(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("foo").getMaxValue(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("foobar").getMinValue(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().get("test1").get("foobar").getMaxValue(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().get("test2").size(), equalTo(1));
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("foobar").getMinValue(), nullValue());
        assertThat(response.getIndicesMergedFieldStats().get("test2").get("foobar").getMaxValue(), nullValue());
    }

    private void indexRange(String index, long from, long to) throws Exception {
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (long value = from; value <= to; value++) {
            requests.add(client().prepareIndex(index, "test").setSource("value", value));
        }
        indexRandom(true, false, requests);
    }

}
