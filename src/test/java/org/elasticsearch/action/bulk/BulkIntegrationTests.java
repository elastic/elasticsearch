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

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class BulkIntegrationTests  extends ElasticsearchIntegrationTest{

    @Test
    public void testBulkIndexCreatesMapping() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/bulk-log.json");
        BulkRequestBuilder bulkBuilder = new BulkRequestBuilder(client());
        bulkBuilder.add(bulkAction.getBytes(Charsets.UTF_8), 0, bulkAction.length(), true, null, null);
        bulkBuilder.execute().actionGet();
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                try {
                    GetMappingsResponse mappingsResponse = client().admin().indices().getMappings(new GetMappingsRequest()).get();
                    return mappingsResponse.getMappings().containsKey("logstash-2014.03.30");
                } catch (Throwable t) {
                    return false;
                }
            }
        });
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                try {
                    GetMappingsResponse mappingsResponse = client().admin().indices().getMappings(new GetMappingsRequest()).get();
                    return mappingsResponse.getMappings().get("logstash-2014.03.30").containsKey("logs");
                } catch (Throwable t) {
                    return false;
                }
            }
        });
        ensureYellow();
        GetMappingsResponse mappingsResponse = client().admin().indices().getMappings(new GetMappingsRequest()).get();
        assertThat(mappingsResponse.mappings().size(), equalTo(1));
        assertTrue(mappingsResponse.getMappings().containsKey("logstash-2014.03.30"));
        assertTrue(mappingsResponse.getMappings().get("logstash-2014.03.30").containsKey("logs"));
    }
}
