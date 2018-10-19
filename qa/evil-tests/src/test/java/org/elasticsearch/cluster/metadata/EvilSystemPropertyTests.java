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
package org.elasticsearch.cluster.metadata;

import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class EvilSystemPropertyTests extends ESTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testMaxNumShards() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            IndexMetaData.buildNumberOfShardsSetting()
                .get(Settings.builder().put("index.number_of_shards", 1025).build()));
        assertEquals("Failed to parse value [1025] for setting [index.number_of_shards] must be <= 1024", exception.getMessage());

        Integer numShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(Settings.builder().put("index.number_of_shards", 100).build());
        assertEquals(100, numShards.intValue());
        int limit = randomIntBetween(1, 10);
        System.setProperty("es.index.max_number_of_shards", Integer.toString(limit));
        try {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                IndexMetaData.buildNumberOfShardsSetting()
                    .get(Settings.builder().put("index.number_of_shards", 11).build()));
            assertEquals("Failed to parse value [11] for setting [index.number_of_shards] must be <= " + limit, e.getMessage());
        } finally {
            System.clearProperty("es.index.max_number_of_shards");
        }
    }

    @SuppressForbidden(reason = "manipulates system property es.query.apply_graph_phrase_limit")
    public void testApplyGraphPhraseLimit() {
        assertFalse(MapperQueryParser.shouldApplyGraphPhraseLimit());
        try {
            System.setProperty("es.query.apply_graph_phrase_limit", "false");
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> MapperQueryParser.shouldApplyGraphPhraseLimit());
            assertThat(exc.getMessage(), containsString("[false] is not a valid value for the JVM option"));

            System.setProperty("es.query.apply_graph_phrase_limit", "lol");
            exc = expectThrows(IllegalArgumentException.class,
                () -> MapperQueryParser.shouldApplyGraphPhraseLimit());
            assertThat(exc.getMessage(), containsString("[lol] is not a valid value for the JVM option"));

            System.setProperty("es.query.apply_graph_phrase_limit", "true");
            assertTrue(MapperQueryParser.shouldApplyGraphPhraseLimit());
        } finally {
            System.clearProperty("es.query.apply_graph_phrase_limit");
        }
    }
}
