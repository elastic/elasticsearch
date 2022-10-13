/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

public class ComposableTemplateIT extends ESIntegTestCase {

    // See: https://github.com/elastic/elasticsearch/issues/58643
    public void testComponentTemplatesCanBeUpdatedAfterRestart() throws Exception {
        ComponentTemplate ct = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "dynamic": false,
              "properties": {
                "foo": {
                  "type": "text"
                }
              }
            }"""), null), 3L, Collections.singletonMap("eggplant", "potato"));
        client().execute(PutComponentTemplateAction.INSTANCE, new PutComponentTemplateAction.Request("my-ct").componentTemplate(ct)).get();

        ComposableIndexTemplate cit = new ComposableIndexTemplate(
            Collections.singletonList("coleslaw"),
            new Template(null, new CompressedXContent("""
                {
                  "dynamic": false,
                  "properties": {
                    "foo": {
                      "type": "keyword"
                    }
                  }
                }"""), null),
            Collections.singletonList("my-ct"),
            4L,
            5L,
            Collections.singletonMap("egg", "bread")
        );
        client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("my-it").indexTemplate(cit)
        ).get();

        internalCluster().fullRestart();
        ensureGreen();

        ComponentTemplate ct2 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "dynamic": true,
              "properties": {
                "foo": {
                  "type": "keyword"
                }
              }
            }"""), null), 3L, Collections.singletonMap("eggplant", "potato"));
        client().execute(PutComponentTemplateAction.INSTANCE, new PutComponentTemplateAction.Request("my-ct").componentTemplate(ct2)).get();

        ComposableIndexTemplate cit2 = new ComposableIndexTemplate(
            Collections.singletonList("coleslaw"),
            new Template(null, new CompressedXContent("""
                {
                  "dynamic": true,
                  "properties": {
                    "foo": {
                      "type": "integer"
                    }
                  }
                }"""), null),
            Collections.singletonList("my-ct"),
            4L,
            5L,
            Collections.singletonMap("egg", "bread")
        );
        client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("my-it").indexTemplate(cit2)
        ).get();
    }
}
