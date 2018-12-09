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

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LegacyBulkWithUpdatesIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    /*
     * Test for https://github.com/elastic/elasticsearch/issues/8365
     */
    public void testBulkUpdateChildMissingParentRouting() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)) // allows for multiple types
                .addMapping("parent", "{\"parent\":{}}", XContentType.JSON)
                .addMapping("child", "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}", XContentType.JSON));
        ensureGreen();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = (
                "{" +
                        "  \"index\" : {" +
                        "    \"_index\" : \"test\"," +
                        "    \"_type\"  : \"parent\"," +
                        "    \"_id\"    : \"parent1\"" +
                        "  }" +
                        "}" +
                        "\n" +
                        "{" +
                        "  \"field1\" : \"value1\"" +
                        "}" +
                        "\n").getBytes(StandardCharsets.UTF_8);

        byte[] addChildOK = (
                "{" +
                        "  \"index\" : {" +
                        "    \"_index\" : \"test\"," +
                        "    \"_type\"  : \"child\"," +
                        "    \"_id\"    : \"child1\"," +
                        "    \"parent\" : \"parent1\"" +
                        "  }" +
                        "}" +
                        "\n" +
                        "{" +
                        "  \"field1\" : \"value1\"" +
                        "}" +
                        "\n").getBytes(StandardCharsets.UTF_8);

        byte[] addChildMissingRouting = (
                "{" +
                        "  \"index\" : {" +
                        "    \"_index\" : \"test\"," +
                        "    \"_type\"  : \"child\"," +
                        "    \"_id\"    : \"child1\"" +
                        "  }" +
                        "}" +
                        "\n" +
                        "{" +
                        "  \"field1\" : \"value1\"" +
                        "}" +
                        "\n").getBytes(StandardCharsets.UTF_8);

        builder.add(addParent, 0, addParent.length, XContentType.JSON);
        builder.add(addChildOK, 0, addChildOK.length, XContentType.JSON);
        builder.add(addChildMissingRouting, 0, addChildMissingRouting.length, XContentType.JSON);
        builder.add(addChildOK, 0, addChildOK.length, XContentType.JSON);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(4));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[3].isFailed(), equalTo(false));
    }

}
