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

package org.elasticsearch.indices;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for custom data path locations and templates
 */
public class IndicesCustomDataPathTests extends ElasticsearchIntegrationTest {

    private volatile String path;

    @Before
    public void setup() {
        path = newTempDirPath().toAbsolutePath().toString();
    }

    @After
    public void teardown() throws Exception {
        IOUtils.deleteFilesIgnoringExceptions(Paths.get(path));
    }

    @Test
    public void testIndexCreatedWithCustomPathAndTemplate() throws Exception {
        final String INDEX = "myindex2";

        logger.info("--> creating an index with data_path [{}]", path);
        ImmutableSettings.Builder sb = ImmutableSettings.builder().put(IndexMetaData.SETTING_DATA_PATH, path);;


        client().admin().indices().prepareCreate(INDEX).setSettings(sb).get();
        ensureGreen(INDEX);

        indexRandom(true, client().prepareIndex(INDEX, "doc", "1").setSource("{\"body\": \"foo\"}"));

        SearchResponse resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));
        assertAcked(client().admin().indices().prepareDelete(INDEX));
        assertPathHasBeenCleared(path);
    }

    private void assertPathHasBeenCleared(String path) throws Exception {
        int count = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (Files.exists(Paths.get(path))) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(path))) {
                for (Path file : stream) {
                    count++;
                    sb.append(file.toAbsolutePath().toString());
                    sb.append("\n");
                }
            }
        }
        sb.append("]");
        assertThat(count + " files exist that should have been cleaned:\n" + sb.toString(), count, equalTo(0));
    }
}
