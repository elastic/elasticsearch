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

package org.elasticsearch.index.mapper.all;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Tests that position_increment_gap is read from the mapper and applies as
 * expected in queries.
 */
public class AllFieldMapperPositionIncrementGapTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

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
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "one two")).get(), 0);

        // Nor with small-ish values
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "one two").slop(5)).get(), 0);
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "one two").slop(50)).get(), 0);

        // But huge-ish values still match
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "one two").slop(500)).get(), 1);
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
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("string", "one two")).get(), 1);
    }

    private static void testGap(Client client, String indexName,
                                String type, int positionIncrementGap) throws IOException {
        client.prepareIndex(indexName, type, "position_gap_test")
            .setSource("string1", "one", "string2", "two three").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        // Baseline - phrase query finds matches in the same field value
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "two three")).get(), 1);

        if (positionIncrementGap > 0) {
            // No match across gaps when slop < position gap
            assertHitCount(client.prepareSearch(indexName)
                .setQuery(new MatchPhraseQueryBuilder("_all", "one two").slop(positionIncrementGap - 1)).get(), 0);
        }

        // Match across gaps when slop >= position gap
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "one two").slop(positionIncrementGap)).get(), 1);
        assertHitCount(client.prepareSearch(indexName)
            .setQuery(new MatchPhraseQueryBuilder("_all", "one two").slop(positionIncrementGap+1)).get(), 1);
    }
}
