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

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;

/**
 * This is test is meant to verify that when upgrading from 6.x version to 7.7 or newer it is able to parse date fields with joda pattern.
 * This test cannot be implemented in yml because in mixed cluster
 * there are 3 options of warnings to be returned (it was refactored few times).
 * A special flag on DocValues is used to indicate that an index was created in 6.x and has a joda pattern.
 * When upgrading from 7.0-7.6 to 7.7 there is no way to tell if a pattern was created in 6.x as this flag cannot be added.
 * @see org.elasticsearch.search.DocValueFormat.DateTime
 */
public class DateFieldsIT extends AbstractRollingTestCase {
    private static final Version UPGRADE_FROM_VERSION =
        Version.fromString(System.getProperty("tests.upgrade_from_version"));

    private static final String V_6_8_1_PLUS_WARNING = "'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.; " +
        "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'." +
        " Prefix your date format with '8' to use the new specifier.";

    private static final String V_7_0_0_PLUS_WARNING = "'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.; " +
        "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'. " +
        "Use new java.time date format specifiers.";

    private static final String V_6_8_0_WARNING = "Use of 'Y' (year-of-era) will change to 'y' in the next major version of Elasticsearch. "
        + "Prefix your date format with '8' to use the new specifier.";

    @BeforeClass
    public static void init(){
        assumeTrue("upgrading from 7.0-7.6 will fail parsing joda formats",
            UPGRADE_FROM_VERSION.before(Version.V_7_0_0));
    }

    public void testJodaBackedDocValueAndDateFields() throws Exception {

        switch (CLUSTER_TYPE) {
            case OLD:
                Request createTestIndex = indexWithDateField("joda_it", "YYYY-MM-dd'T'HH:mm:ssZZ");

                Version minVersion = getMinVersion();
                if (minVersion.equals(Version.V_6_8_0)) {
                    createTestIndex.setOptions(expectWarnings(V_6_8_0_WARNING));
                } else if (minVersion.onOrAfter(Version.V_6_8_1) && minVersion.before(Version.V_7_0_0)) {
                    createTestIndex.setOptions(expectWarnings(V_6_8_1_PLUS_WARNING));
                } else {
                    createTestIndex.setOptions(expectWarnings(V_7_0_0_PLUS_WARNING));
                }

                Response resp = client().performRequest(createTestIndex);
                assertEquals(200, resp.getStatusLine().getStatusCode());

                postNewDoc("joda_it/_doc");

                break;
            case MIXED:
                postNewDoc("joda_it/_doc");

                Request search = dateRangeSearch("joda_it/_search");

                RequestOptions options = expectVersionSpecificWarnings(
                    consumer -> consumer.compatible(V_6_8_0_WARNING, V_6_8_1_PLUS_WARNING, V_7_0_0_PLUS_WARNING));

                search.setOptions(options);

                Response searchResp = client().performRequest(search);
                assertEquals(200, searchResp.getStatusLine().getStatusCode());
                break;
            case UPGRADED:
                postNewDoc("joda_it/_doc");
                search = dateRangeSearch("joda_it/_search");
                //somehow  this can at times not have a warning..

                search.setOptions(expectWarnings(V_7_0_0_PLUS_WARNING));
                searchResp = client().performRequest(search);
                assertEquals(200, searchResp.getStatusLine().getStatusCode());
                break;
        }
    }

    public void testJavaBackedDocValueAndDateFields() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD:
                Request createTestIndex = indexWithDateField("java_it", "8yyyy-MM-dd'T'HH:mm:ssXXX");
                Response resp = client().performRequest(createTestIndex);
                assertEquals(200, resp.getStatusLine().getStatusCode());

                postNewDoc("java_it/_doc");

                break;
            case MIXED:
                postNewDoc("java_it/_doc");

                Request search = dateRangeSearch("java_it/_search");
                Response searchResp = client().performRequest(search);
                assertEquals(200, searchResp.getStatusLine().getStatusCode());
                break;
            case UPGRADED:
                postNewDoc("java_it/_doc");

                search = dateRangeSearch("java_it/_search");
                searchResp = client().performRequest(search);
                assertEquals(200, searchResp.getStatusLine().getStatusCode());
                break;
        }
    }

    private Request dateRangeSearch(String endpoint) {
        Request search = new Request("GET", endpoint);
        search.setJsonEntity("" +
                "{\n" +
                "  \"sort\": \"datetime\",\n" +
                "  \"query\": {\n" +
                "    \"range\": {\n" +
                "      \"datetime\": {\n" +
                "        \"gte\": \"2020-01-01T00:00:00+01:00\",\n" +
                "        \"lte\": \"2020-01-02T00:00:00+01:00\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n"
        );
        return search;
    }

    private Request indexWithDateField(String indexName, String format) {
        Request createTestIndex = new Request("PUT", indexName);
        createTestIndex.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"index.number_of_shards\": 3\n" +
            "  },\n" +
            "  \"mappings\": {\n" +
            "      \"properties\": {\n" +
            "        \"datetime\": {\n" +
            "          \"type\": \"date\",\n" +
            "          \"format\": \"" + format + "\"\n" +
            "        }\n" +
            "      }\n" +
            "  }\n" +
            "}"
        );
        return createTestIndex;
    }

    private Version getMinVersion() throws IOException {
        Version minNodeVersion = null;
        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
        for (Map.Entry<?, ?> node : nodes.entrySet()) {
            Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
            Version nodeVersion = Version.fromString(nodeInfo.get("version").toString());
            if (minNodeVersion == null) {
                minNodeVersion = nodeVersion;
            } else if (nodeVersion.before(minNodeVersion)) {
                minNodeVersion = nodeVersion;
            }
        }
        return minNodeVersion;
    }

    private void postNewDoc(String endpoint) throws IOException {
        Request putDoc = new Request("POST", endpoint);
        putDoc.setJsonEntity("{\n" +
            "  \"datetime\": \"2020-01-01T01:01:01+01:00\"\n" +
            "}"
        );
        Response resp = client().performRequest(putDoc);
        assertEquals(201, resp.getStatusLine().getStatusCode());
    }

}
