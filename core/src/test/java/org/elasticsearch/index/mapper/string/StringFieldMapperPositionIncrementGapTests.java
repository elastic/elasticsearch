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

package org.elasticsearch.index.mapper.string;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that position_increment_gap is read from the mapper and applies as
 * expected in queries.
 */
public class StringFieldMapperPositionIncrementGapTests extends ESSingleNodeTestCase {
    /**
     * The default position_increment_gap should be large enough that most
     * "sensible" queries phrase slops won't match across values.
     */
    public void testDefault() throws IOException {
        assertGapIsOneHundred(client(), "test", "test");
    }

    /**
     * Asserts that the post-2.0 default is being applied.
     */
    public static void assertGapIsOneHundred(Client client, String indexName, String type) throws IOException {
        testGap(client, indexName, type, 100);

        // No match across gap using default slop with default positionIncrementGap
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two")).get(), 0);

        // Nor with small-ish values
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(5)).get(), 0);
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(50)).get(), 0);

        // But huge-ish values still match
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(500)).get(), 1);
    }

    public void testZero() throws IOException {
        setupGapInMapping(0);
        assertGapIsZero(client(), "test", "test");
    }

    /**
     * Asserts that the pre-2.0 default has been applied or explicitly
     * configured.
     */
    public static void assertGapIsZero(Client client, String indexName, String type) throws IOException {
        testGap(client, indexName, type, 0);
        /*
         * Phrases match across different values using default slop with pre-2.0 default
         * position_increment_gap.
         */
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two")).get(), 1);
    }

    public void testLargerThanDefault() throws IOException {
        setupGapInMapping(10000);
        testGap(client(), "test", "test", 10000);
    }

    public void testSmallerThanDefault() throws IOException {
        setupGapInMapping(2);
        testGap(client(), "test", "test", 2);
    }

    public void testNegativeIsError() throws IOException {
        try {
            setupGapInMapping(-1);
            fail("Expected an error");
        } catch (MapperParsingException e) {
            assertThat(ExceptionsHelper.detailedMessage(e), containsString("positions_increment_gap less than 0 aren't allowed"));
        }
    }

    /**
     * Tests that the default actually defaults to the position_increment_gap
     * configured in the analyzer. This behavior is very old and a little
     * strange but not worth breaking some thought.
     */
    public void testDefaultDefaultsToAnalyzer() throws IOException {
        XContentBuilder settings = XContentFactory.jsonBuilder().startObject().startObject("analysis").startObject("analyzer")
                .startObject("gappy");
        settings.field("type", "custom");
        settings.field("tokenizer", "standard");
        settings.field("position_increment_gap", 2);
        setupAnalyzer(settings, "gappy");
        testGap(client(), "test", "test", 2);
    }

    /**
     * Build an index named "test" with a field named "string" with the provided
     * positionIncrementGap that uses the standard analyzer.
     */
    private void setupGapInMapping(int positionIncrementGap) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("string");
        mapping.field("type", "string");
        mapping.field("position_increment_gap", positionIncrementGap);
        client().admin().indices().prepareCreate("test").addMapping("test", mapping).get();
    }

    /**
     * Build an index named "test" with the provided settings and and a field
     * named "string" that uses the specified analyzer and default
     * position_increment_gap.
     */
    private void setupAnalyzer(XContentBuilder settings, String analyzer) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("string");
        mapping.field("type", "string");
        mapping.field("analyzer", analyzer);
        client().admin().indices().prepareCreate("test").addMapping("test", mapping).setSettings(settings).get();
    }

    private static void testGap(Client client, String indexName, String type, int positionIncrementGap) throws IOException {
        client.prepareIndex(indexName, type, "position_gap_test").setSource("string", Arrays.asList("one", "two three")).setRefresh(true).get();

        // Baseline - phrase query finds matches in the same field value
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "two three")).get(), 1);

        if (positionIncrementGap > 0) {
            // No match across gaps when slop < position gap
            assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(positionIncrementGap - 1)).get(),
                    0);
        }

        // Match across gaps when slop >= position gap
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(positionIncrementGap)).get(), 1);
        assertHitCount(client.prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(positionIncrementGap + 1)).get(), 1);
    }
}
