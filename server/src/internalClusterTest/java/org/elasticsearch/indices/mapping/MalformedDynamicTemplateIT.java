/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentType;

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
    @UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT)
    @AwaitsFix(bugUrl = "this is testing 7.x specific compatibility which may be n/a now after 9.0 bump")
    public void testBWCMalformedDynamicTemplate() {
        // this parameter is not supported by "keyword" field type
        String mapping = """
            { "dynamic_templates": [
                  {
                    "my_template": {
                      "mapping": {
                        "ignore_malformed": true,
                        "type": "keyword"
                      },
                      "path_match": "*"
                    }
                  }
                ]
              }
            }}""";
        String indexName = "malformed_dynamic_template";
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("number_of_shards", 1)
                    .put("index.version.created", IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0))
            ).setMapping(mapping)
        );
        prepareIndex(indexName).setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        assertNoFailures((indicesAdmin().prepareRefresh(indexName)).get());
        assertHitCount(prepareSearch(indexName), 1);

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            prepareCreate("malformed_dynamic_template_8.0").setSettings(
                Settings.builder().put(indexSettings()).put("number_of_shards", 1).put("index.version.created", IndexVersion.current())
            ).setMapping(mapping)
        );
        assertThat(ex.getMessage(), containsString("dynamic template [my_template] has invalid content"));
    }

}
