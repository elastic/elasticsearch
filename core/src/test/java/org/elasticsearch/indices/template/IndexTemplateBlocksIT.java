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

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndexTemplateBlocksIT extends ESIntegTestCase {

    @Test
    public void testIndexTemplatesWithBlocks() throws IOException {
        // creates a simple index template
        client().admin().indices().preparePutTemplate("template_blocks")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        try {
            setClusterReadOnly(true);

            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("template_blocks").execute().actionGet();
            assertThat(response.getIndexTemplates(), hasSize(1));

            assertBlocked(client().admin().indices().preparePutTemplate("template_blocks_2")
                    .setTemplate("block*")
                    .setOrder(0)
                    .addAlias(new Alias("alias_1")));

            assertBlocked(client().admin().indices().prepareDeleteTemplate("template_blocks"));

        } finally {
            setClusterReadOnly(false);
        }
    }
}
