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

package org.elasticsearch.upgrades;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests to run before and after a full cluster restart. This is run twice,
 * one with {@code tests.is_old_cluster} set to {@code true} against a cluster
 * of an older version. The cluster is shutdown and a cluster of the new
 * version is started with the same data directories and then this is rerun
 * with {@code tests.is_old_cluster} set to {@code false}.
 */
public class FullClusterRestartIT extends ESRestTestCase {
    private final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));
    private final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private final boolean supportsLenientBooleans = oldClusterVersion.onOrAfter(Version.V_6_0_0_alpha1);

    private String index;

    @Before
    public void setIndex() {
        index = getTestName().toLowerCase(Locale.ROOT);
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    public void testSearch() throws Exception {
        int count;
        if (runningAgainstOldCluster) {
            XContentBuilder mappingsAndSettings = jsonBuilder();
            mappingsAndSettings.startObject();
            {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 0);
                mappingsAndSettings.endObject();
            }
            {
                mappingsAndSettings.startObject("mappings");
                mappingsAndSettings.startObject("doc");
                mappingsAndSettings.startObject("properties");
                {
                    mappingsAndSettings.startObject("string");
                    mappingsAndSettings.field("type", "text");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("dots_in_field_names");
                    mappingsAndSettings.field("type", "text");
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();
                mappingsAndSettings.endObject();
                mappingsAndSettings.endObject();
            }
            mappingsAndSettings.endObject();
            client().performRequest("PUT", "/" + index, Collections.emptyMap(),
                new StringEntity(mappingsAndSettings.string(), ContentType.APPLICATION_JSON));

            count = randomIntBetween(2000, 3000);
            indexRandomDocuments(count, true, true, i -> {
                return JsonXContent.contentBuilder().startObject()
                .field("string", randomAlphaOfLength(10))
                .field("int", randomInt(100))
                .field("float", randomFloat())
                // be sure to create a "proper" boolean (True, False) for the first document so that automapping is correct
                .field("bool", i > 0 && supportsLenientBooleans ? randomLenientBoolean() : randomBoolean())
                .field("field.with.dots", randomAlphaOfLength(10))
                // TODO a binary field
                .endObject();
            });
            refresh();
        } else {
            count = countOfIndexedRandomDocuments();
        }
        assertBasicSearchWorks(count);
    }

    void assertBasicSearchWorks(int count) throws IOException {
        logger.info("--> testing basic search");
        Map<String, Object> response = toMap(client().performRequest("GET", "/" + index + "/_search"));
        assertNoFailures(response);
        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        logger.info("Found {} in old index", numDocs);
        assertEquals(count, numDocs);

        logger.info("--> testing basic search with sort");
        String searchRequestBody = "{ \"sort\": [{ \"int\" : \"asc\" }]}";
        response = toMap(client().performRequest("GET", "/" + index + "/_search", Collections.emptyMap(),
            new StringEntity(searchRequestBody, ContentType.APPLICATION_JSON)));
        assertNoFailures(response);
        numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertEquals(count, numDocs);

        logger.info("--> testing exists filter");
        searchRequestBody = "{ \"query\": { \"exists\" : {\"field\": \"string\"} }}";
        response = toMap(client().performRequest("GET", "/" + index + "/_search", Collections.emptyMap(),
            new StringEntity(searchRequestBody, ContentType.APPLICATION_JSON)));
        assertNoFailures(response);
        numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertEquals(count, numDocs);

        searchRequestBody = "{ \"query\": { \"exists\" : {\"field\": \"field.with.dots\"} }}";
        response = toMap(client().performRequest("GET", "/" + index + "/_search", Collections.emptyMap(),
            new StringEntity(searchRequestBody, ContentType.APPLICATION_JSON)));
        assertNoFailures(response);
        numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertEquals(count, numDocs);
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    static Map<String, Object> toMap(String response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    static void assertNoFailures(Map<String, Object> response) {
        int failed = (int) XContentMapValues.extractValue("_shards.failed", response);
        assertEquals(0, failed);
    }

    /**
     * Tests that a single document survives. Super basic smoke test.
     */
    public void testSingleDoc() throws IOException {
        String docLocation = "/" + index + "/doc/1";
        String doc = "{\"test\": \"test\"}";

        if (runningAgainstOldCluster) {
            client().performRequest("PUT", docLocation, singletonMap("refresh", "true"),
                    new StringEntity(doc, ContentType.APPLICATION_JSON));
        }

        assertThat(EntityUtils.toString(client().performRequest("GET", docLocation).getEntity()), containsString(doc));
    }

    /**
     * Tests recovery of an index with or without a translog and the
     * statistics we gather about that. 
     */
    public void testRecovery() throws IOException {
        int count;
        boolean shouldHaveTranslog;
        if (runningAgainstOldCluster) {
            count = between(200, 300);
            /* We've had bugs in the past where we couldn't restore
             * an index without a translog so we randomize whether
             * or not we have one. */
            shouldHaveTranslog = randomBoolean();

            indexRandomDocuments(count, true, true, i -> jsonBuilder().startObject().field("field", "value").endObject());
            // Explicitly flush so we're sure to have a bunch of documents in the Lucene index
            client().performRequest("POST", "/_flush");
            if (shouldHaveTranslog) {
                // Update a few documents so we are sure to have a translog
                indexRandomDocuments(count / 10, false /* Flushing here would invalidate the whole thing....*/, false,
                    i -> jsonBuilder().startObject().field("field", "value").endObject());
            }
            saveInfoDocument("should_have_translog", Boolean.toString(shouldHaveTranslog));
        } else {
            count = countOfIndexedRandomDocuments();
            shouldHaveTranslog = Booleans.parseBoolean(loadInfoDocument("should_have_translog"));
        }

        // Count the documents in the index to make sure we have as many as we put there
        String countResponse = EntityUtils.toString(
                client().performRequest("GET", "/" + index + "/_search", singletonMap("size", "0")).getEntity());
        assertThat(countResponse, containsString("\"total\":" + count));

        if (false == runningAgainstOldCluster) {
            boolean restoredFromTranslog = false;
            boolean foundPrimary = false;
            Map<String, String> params = new HashMap<>();
            params.put("h", "index,shard,type,stage,translog_ops_recovered");
            params.put("s", "index,shard,type");
            String recoveryResponse = EntityUtils.toString(client().performRequest("GET", "/_cat/recovery/" + index, params).getEntity());
            for (String line : recoveryResponse.split("\n")) {
                // Find the primaries
                foundPrimary = true;
                if (false == line.contains("done") && line.contains("existing_store")) {
                    continue;
                }
                /* Mark if we see a primary that looked like it restored from the translog.
                 * Not all primaries will look like this all the time because we modify
                 * random documents when we want there to be a translog and they might
                 * not be spread around all the shards. */
                Matcher m = Pattern.compile("(\\d+)$").matcher(line);
                assertTrue(line, m.find());
                int translogOps = Integer.parseInt(m.group(1));
                if (translogOps > 0) {
                    restoredFromTranslog = true;
                }
            }
            assertTrue("expected to find a primary but didn't\n" + recoveryResponse, foundPrimary);
            assertEquals("mismatch while checking for translog recovery\n" + recoveryResponse, shouldHaveTranslog, restoredFromTranslog);

            String currentLuceneVersion = Version.CURRENT.luceneVersion.toString();
            String bwcLuceneVersion = oldClusterVersion.luceneVersion.toString();
            if (shouldHaveTranslog && false == currentLuceneVersion.equals(bwcLuceneVersion)) {
                int numCurrentVersion = 0;
                int numBwcVersion = 0;
                params.clear();
                params.put("h", "prirep,shard,index,version");
                params.put("s", "prirep,shard,index");
                String segmentsResponse = EntityUtils.toString(
                        client().performRequest("GET", "/_cat/segments/" + index, params).getEntity());
                for (String line : segmentsResponse.split("\n")) {
                    if (false == line.startsWith("p")) {
                        continue;
                    }
                    Matcher m = Pattern.compile("(\\d+\\.\\d+\\.\\d+)$").matcher(line);
                    assertTrue(line, m.find());
                    String version = m.group(1);
                    if (currentLuceneVersion.equals(version)) {
                        numCurrentVersion++;
                    } else if (bwcLuceneVersion.equals(version)) {
                        numBwcVersion++;
                    } else {
                        fail("expected version to be one of [" + currentLuceneVersion + "," + bwcLuceneVersion + "] but was " + line);
                    }
                }
                assertNotEquals("expected at least 1 current segment after translog recovery", 0, numCurrentVersion);
                assertNotEquals("expected at least 1 old segment", 0, numBwcVersion);
            }
        }
    }

    @AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/25203")
    public void testSnapshotRestore() throws IOException {
        int count;
        if (runningAgainstOldCluster) {
            count = between(200, 300);
            indexRandomDocuments(count, true, true, i -> jsonBuilder().startObject().field("field", "value").endObject());

            // Create the repo and the snapshot
            XContentBuilder repoConfig = JsonXContent.contentBuilder().startObject(); {
                repoConfig.field("type", "fs");
                repoConfig.startObject("settings"); {
                    repoConfig.field("compress", randomBoolean());
                    repoConfig.field("location", System.getProperty("tests.path.repo"));
                }
                repoConfig.endObject();
            }
            repoConfig.endObject();
            client().performRequest("PUT", "/_snapshot/repo", emptyMap(),
                    new StringEntity(repoConfig.string(), ContentType.APPLICATION_JSON));

            XContentBuilder snapshotConfig = JsonXContent.contentBuilder().startObject(); {
                snapshotConfig.field("indices", index);
            }
            snapshotConfig.endObject();
            client().performRequest("PUT", "/_snapshot/repo/snap", singletonMap("wait_for_completion", "true"),
                    new StringEntity(snapshotConfig.string(), ContentType.APPLICATION_JSON));

            // Refresh the index so the count doesn't fail
            refresh();
        } else {
            count = countOfIndexedRandomDocuments();
        }

        // Count the documents in the index to make sure we have as many as we put there
        String countResponse = EntityUtils.toString(
                client().performRequest("GET", "/" + index + "/_search", singletonMap("size", "0")).getEntity());
        assertThat(countResponse, containsString("\"total\":" + count));

        if (false == runningAgainstOldCluster) {
            /* Remove any "restored" indices from the old cluster run of this test.
             * We intentionally don't remove them while running this against the
             * old cluster so we can test starting the node with a restored index
             * in the cluster. */
            client().performRequest("DELETE", "/restored_*");
        }

        // Check the metadata, especially the version
        String response = EntityUtils.toString(
                client().performRequest("GET", "/_snapshot/repo/_all", singletonMap("verbose", "true")).getEntity());
        Map<String, Object> map = toMap(response);
        assertEquals(response, singletonList("snap"), XContentMapValues.extractValue("snapshots.snapshot", map));
        assertEquals(response, singletonList("SUCCESS"), XContentMapValues.extractValue("snapshots.state", map));
        assertEquals(response, singletonList(oldClusterVersion.toString()), XContentMapValues.extractValue("snapshots.version", map));

        XContentBuilder restoreCommand = JsonXContent.contentBuilder().startObject();
        restoreCommand.field("include_global_state", randomBoolean());
        restoreCommand.field("indices", index);
        restoreCommand.field("rename_pattern", index);
        restoreCommand.field("rename_replacement", "restored_" + index);
        restoreCommand.endObject();
        client().performRequest("POST", "/_snapshot/repo/snap/_restore", singletonMap("wait_for_completion", "true"),
                new StringEntity(restoreCommand.string(), ContentType.APPLICATION_JSON));

        countResponse = EntityUtils.toString(
                client().performRequest("GET", "/restored_" + index + "/_search", singletonMap("size", "0")).getEntity());
        assertThat(countResponse, containsString("\"total\":" + count));
        
    }

    // TODO tests for upgrades after shrink. We've had trouble with shrink in the past.

    private void indexRandomDocuments(int count, boolean flushAllowed, boolean saveInfo,
                                      CheckedFunction<Integer, XContentBuilder, IOException> docSupplier) throws IOException {
        logger.info("Indexing {} random documents", count);
        for (int i = 0; i < count; i++) {
            logger.debug("Indexing document [{}]", i);
            client().performRequest("POST", "/" + index + "/doc/" + i, emptyMap(),
                    new StringEntity(docSupplier.apply(i).string(), ContentType.APPLICATION_JSON));
            if (rarely()) {
                refresh();
            }
            if (flushAllowed && rarely()) {
                logger.debug("Flushing [{}]", index);
                client().performRequest("POST", "/" + index + "/_flush");
            }
        }
        if (saveInfo) {
            saveInfoDocument("count", Integer.toString(count));
        }
    }

    private int countOfIndexedRandomDocuments() throws IOException {
        return Integer.parseInt(loadInfoDocument("count"));
    }

    private void saveInfoDocument(String type, String value) throws IOException {
        XContentBuilder infoDoc = JsonXContent.contentBuilder().startObject();
        infoDoc.field("value", value);
        infoDoc.endObject();
        // Only create the first version so we know how many documents are created when the index is first created
        Map<String, String> params = singletonMap("op_type", "create");
        client().performRequest("PUT", "/info/doc/" + index + "_" + type, params,
                new StringEntity(infoDoc.string(), ContentType.APPLICATION_JSON));
    }

    private String loadInfoDocument(String type) throws IOException {
        String doc = EntityUtils.toString(
                client().performRequest("GET", "/info/doc/" + index + "_" + type, singletonMap("filter_path", "_source")).getEntity());
        Matcher m = Pattern.compile("\"value\":\"(.+)\"").matcher(doc);
        assertTrue(doc, m.find());
        return m.group(1);
    }

    private Object randomLenientBoolean() {
        return randomFrom(new Object[] {"off", "no", "0", 0, "false", false, "on", "yes", "1", 1, "true", true});
    }

    private void refresh() throws IOException {
        logger.debug("Refreshing [{}]", index);
        client().performRequest("POST", "/" + index + "/_refresh");
    }
}
