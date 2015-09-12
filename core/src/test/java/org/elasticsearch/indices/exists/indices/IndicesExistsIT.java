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

package org.elasticsearch.indices.exists.indices;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.hamcrest.Matchers.equalTo;

public class IndicesExistsIT extends ESIntegTestCase {

    @Test
    // Indices exists never throws IndexMissingException, the indices options control its behaviour (return true or false)
    public void testIndicesExists() throws Exception {
        assertThat(client().admin().indices().prepareExists("foo").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foo").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("foo*").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foo*").setIndicesOptions(IndicesOptions.fromOptions(false, true, true, false)).get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("_all").get().isExists(), equalTo(false));

        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        assertThat(client().admin().indices().prepareExists("foo*").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("bar*").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("bar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("_all").get().isExists(), equalTo(true));
    }

    @Test
    public void testIndicesExistsWithBlocks() {
        createIndex("ro");
        ensureYellow();

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("ro", blockSetting);
                assertThat(client().admin().indices().prepareExists("ro").execute().actionGet().isExists(), equalTo(true));
            } finally {
                disableIndexBlock("ro", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
            assertThat(client().admin().indices().prepareExists("ro").execute().actionGet().isExists(), equalTo(true));
            fail("Exists should fail when " + IndexMetaData.SETTING_BLOCKS_METADATA + " is true");
        } catch (ClusterBlockException e) {
            // Ok, a ClusterBlockException is expected
        } finally {
            disableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
        }
    }
}
