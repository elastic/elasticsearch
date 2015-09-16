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

import java.nio.charset.StandardCharsets;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;

public class BulkIntegrationIT extends ESIntegTestCase {

    @Test
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
}
