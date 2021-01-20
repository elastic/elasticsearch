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

package org.elasticsearch.indices.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;

public class MalformedDynamicTemplateIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    /**
     * Check that we can index a document into an 7.x index with a matching dynamic template that
     * contains unknown parameters. We were able to create those templates in 7.x still, so we need
     * to be able to index new documents into them. Indexing should issue a deprecation warning though.
     */
    public void testBWCMalformedDynamicTemplate() {
        String mapping = "{ \"dynamic_templates\": [\n"
            + "      {\n"
            + "        \"my_template\": {\n"
            + "          \"mapping\": {\n"
            + "            \"ignore_malformed\": true,\n" // this parameter is not supported by "keyword" field type
            + "            \"type\": \"keyword\"\n"
            + "          },\n"
            + "          \"path_match\": \"*\"\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}}";
        String indexName = "malformed_dynamic_template";
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put(indexSettings())
            .put("number_of_shards", 1)
            .put("index.version.created", VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0))
        ).setMapping(mapping).get());
        client().prepareIndex(indexName).setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        assertNoFailures((client().admin().indices().prepareRefresh(indexName)).get());
        assertHitCount(client().prepareSearch(indexName).get(), 1);

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("malformed_dynamic_template_8.0").setSettings(
                Settings.builder().put(indexSettings()).put("number_of_shards", 1).put("index.version.created", Version.CURRENT)
            ).setMapping(mapping).get()
        );
        assertThat(ex.getMessage(), containsString("dynamic template [my_template] has invalid content"));
    }

}