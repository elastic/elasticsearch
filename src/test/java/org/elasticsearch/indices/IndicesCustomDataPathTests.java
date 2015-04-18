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
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for custom data path locations and templates
 */
public class IndicesCustomDataPathTests extends ElasticsearchIntegrationTest {

    private String path;

    @Before
    public void setup() {
        path = createTempDir().toAbsolutePath().toString();
    }

    @After
    public void teardown() throws Exception {
        IOUtils.deleteFilesIgnoringExceptions(PathUtils.get(path));
    }

    @Test
    @TestLogging("_root:DEBUG,index:TRACE")
    public void testDataPathCanBeChanged() throws Exception {
        final String INDEX = "idx";
        Path root = createTempDir();
        Path startDir = root.resolve("start");
        Path endDir = root.resolve("end");
        logger.info("--> start dir: [{}]", startDir.toAbsolutePath().toString());
        logger.info("-->   end dir: [{}]", endDir.toAbsolutePath().toString());
        // temp dirs are automatically created, but the end dir is what
        // startDir is going to be renamed as, so it needs to be deleted
        // otherwise we get all sorts of errors about the directory
        // already existing
        IOUtils.rm(endDir);

        ImmutableSettings.Builder sb = ImmutableSettings.builder().put(IndexMetaData.SETTING_DATA_PATH,
                startDir.toAbsolutePath().toString());
        ImmutableSettings.Builder sb2 = ImmutableSettings.builder().put(IndexMetaData.SETTING_DATA_PATH,
                endDir.toAbsolutePath().toString());

        logger.info("--> creating an index with data_path [{}]", startDir.toAbsolutePath().toString());
        client().admin().indices().prepareCreate(INDEX).setSettings(sb).get();
        ensureGreen(INDEX);

        indexRandom(true, client().prepareIndex(INDEX, "doc", "1").setSource("{\"body\": \"foo\"}"));

        SearchResponse resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        logger.info("--> closing the index [{}]", INDEX);
        client().admin().indices().prepareClose(INDEX).get();
        logger.info("--> index closed, re-opening...");
        client().admin().indices().prepareOpen(INDEX).get();
        logger.info("--> index re-opened");
        ensureGreen(INDEX);

        resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        // Now, try closing and changing the settings

        logger.info("--> closing the index [{}]", INDEX);
        client().admin().indices().prepareClose(INDEX).get();

        logger.info("--> moving data on disk [{}] to [{}]", startDir.getFileName(), endDir.getFileName());
        assert Files.exists(endDir) == false : "end directory should not exist!";
        Files.move(startDir, endDir, StandardCopyOption.REPLACE_EXISTING);

        logger.info("--> updating settings...");
        client().admin().indices().prepareUpdateSettings(INDEX)
                .setSettings(sb2)
                .setIndicesOptions(IndicesOptions.fromOptions(true, false, true, true))
                .get();

        assert Files.exists(startDir) == false : "start dir shouldn't exist";

        logger.info("--> settings updated and files moved, re-opening index");
        client().admin().indices().prepareOpen(INDEX).get();
        logger.info("--> index re-opened");
        ensureGreen(INDEX);

        resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        assertAcked(client().admin().indices().prepareDelete(INDEX));
        assertPathHasBeenCleared(startDir.toAbsolutePath().toString());
        assertPathHasBeenCleared(endDir.toAbsolutePath().toString());
    }

    @Test
    public void testIndexCreatedWithCustomPathAndTemplate() throws Exception {
        final String INDEX = "myindex2";

        logger.info("--> creating an index with data_path [{}]", path);
        ImmutableSettings.Builder sb = ImmutableSettings.builder().put(IndexMetaData.SETTING_DATA_PATH, path);

        client().admin().indices().prepareCreate(INDEX).setSettings(sb).get();
        ensureGreen(INDEX);

        indexRandom(true, client().prepareIndex(INDEX, "doc", "1").setSource("{\"body\": \"foo\"}"));

        SearchResponse resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));
        assertAcked(client().admin().indices().prepareDelete(INDEX));
        assertPathHasBeenCleared(path);
    }
}
