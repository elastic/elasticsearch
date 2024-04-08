/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndexTemplateBlocksIT extends ESIntegTestCase {
    public void testIndexTemplatesWithBlocks() throws IOException {
        // creates a simple index template
        indicesAdmin().preparePutTemplate("template_blocks")
            .setPatterns(Collections.singletonList("te*"))
            .setOrder(0)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "text")
                    .field("store", true)
                    .endObject()
                    .startObject("field2")
                    .field("type", "keyword")
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        try {
            setClusterReadOnly(true);

            GetIndexTemplatesResponse response = indicesAdmin().prepareGetTemplates("template_blocks").get();
            assertThat(response.getIndexTemplates(), hasSize(1));

            assertBlocked(
                indicesAdmin().preparePutTemplate("template_blocks_2")
                    .setPatterns(Collections.singletonList("block*"))
                    .setOrder(0)
                    .addAlias(new Alias("alias_1"))
            );

            assertBlocked(indicesAdmin().prepareDeleteTemplate("template_blocks"));

        } finally {
            setClusterReadOnly(false);
        }
    }
}
