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

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class BulkIntegrationIT extends ESIntegTestCase {
    public void testBulkIndexCreatesMapping() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/bulk-log.json");
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        bulkBuilder.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
        bulkBuilder.get();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings().get();
                assertTrue(mappingsResponse.getMappings().containsKey("logstash-2014.03.30"));
                assertTrue(mappingsResponse.getMappings().get("logstash-2014.03.30").containsKey("logs"));
            }
        });
    }

    public void testBulkRequestWithDeleteDoesNotCreateMapping() {
        BulkItemResponse[] items = client().prepareBulk().add(new DeleteRequest("test", "test", "test")).get().getItems();
        assertThat(items.length, equalTo(1));
        assertTrue(items[0].isFailed());
        assertThat(items[0].getFailure().getCause(), instanceOf(IndexNotFoundException.class));
        assertThat(client().admin().cluster().prepareState().all().get().getState().getMetaData().getIndices().size(), equalTo(0));
    }

    public void testBulkRequestWithDeleteAndExternalVersioningCreatesMapping() {
        BulkItemResponse[] items = client().prepareBulk().add(
            new DeleteRequest("test", "test", "test")
                .version(randomIntBetween(0, 1000))
                .versionType(randomFrom(VersionType.EXTERNAL, VersionType.EXTERNAL_GTE)))
            .get().getItems();
        assertThat(items.length, equalTo(1));
        assertFalse(items[0].isFailed());
        assertThat(client().admin().cluster().prepareState().all().get().getState().getMetaData().index("test"), notNullValue());
    }
}
