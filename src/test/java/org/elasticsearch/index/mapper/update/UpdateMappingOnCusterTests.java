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

package org.elasticsearch.index.mapper.update;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;


public class UpdateMappingOnCusterTests extends ElasticsearchIntegrationTest {

    private static final String INDEX = "index";
    private static final String TYPE = "type";

    // checks if the setting for timestamp and size are kept even if disabled
    @Test
    public void testDisabledSizeTimestampIndexDoNotLooseMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/default_mapping_with_disabled_root_types.json");
        prepareCreate(INDEX).addMapping(TYPE, mapping).get();
        GetMappingsResponse mappingsBeforeGreen = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        ensureGreen(INDEX);
        // make sure all nodes have same cluster state
        compareMappingOnNodes(mappingsBeforeGreen);
    }

    private void compareMappingOnNodes(GetMappingsResponse previousMapping) {
        // make sure all nodes have same cluster state
        for (Client client : cluster()) {
            GetMappingsResponse currentMapping = client.admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).setLocal(true).get();
            assertThat(previousMapping.getMappings().get(INDEX).get(TYPE).source(), equalTo(currentMapping.getMappings().get(INDEX).get(TYPE).source()));
        }
    }
}
