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

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.SUITE)
public class IndexTemplateFilteringIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TestPlugin.class);
    }

    public void testTemplateFiltering() throws Exception {
        client().admin().indices().preparePutTemplate("template1")
                .setTemplate("test*")
                .addMapping("type1", "field1", "type=text").get();

        client().admin().indices().preparePutTemplate("template2")
                .setTemplate("test*")
                .addMapping("type2", "field2", "type=text").get();

        client().admin().indices().preparePutTemplate("template3")
                .setTemplate("no_match")
                .addMapping("type3", "field3", "type=text").get();

        assertAcked(prepareCreate("test"));

        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test").get();
        assertThat(response, notNullValue());
        ImmutableOpenMap<String, MappingMetaData> metadata = response.getMappings().get("test");
        assertThat(metadata.size(), is(1));
        assertThat(metadata.get("type2"), notNullValue());
    }

    public static class TestFilter implements IndexTemplateFilter {
        @Override
        public boolean apply(CreateIndexClusterStateUpdateRequest request, IndexTemplateMetaData template) {
            //make sure that no_match template is filtered out before the custom filters as it doesn't match the index name
            return (template.name().equals("template2") || template.name().equals("no_match"));
        }
    }

    public static class TestPlugin extends Plugin {
        public void onModule(ClusterModule module) {
            module.registerIndexTemplateFilter(TestFilter.class);
        }
    }
}
