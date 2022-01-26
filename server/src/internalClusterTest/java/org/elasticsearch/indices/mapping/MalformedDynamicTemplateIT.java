/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

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
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("number_of_shards", 1)
                    .put("index.version.created", VersionUtils.randomCompatibleVersion(random(), Version.CURRENT))
            ).addMapping("_doc", mapping, XContentType.JSON).get()
        );
        client().prepareIndex(indexName, "_doc").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        assertNoFailures((client().admin().indices().prepareRefresh(indexName)).get());
        assertHitCount(client().prepareSearch(indexName).get(), 1);
    }

}
