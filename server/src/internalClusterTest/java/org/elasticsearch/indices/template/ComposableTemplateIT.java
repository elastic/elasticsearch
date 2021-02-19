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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ComposableTemplateIT extends ESIntegTestCase {

    // See: https://github.com/elastic/elasticsearch/issues/58643
    public void testComponentTemplatesCanBeUpdatedAfterRestart() throws Exception {
        ComponentTemplate ct = new ComponentTemplate(new Template(null, new CompressedXContent("{\n" +
            "      \"dynamic\": false,\n" +
            "      \"properties\": {\n" +
            "        \"foo\": {\n" +
            "          \"type\": \"text\"\n" +
            "        }\n" +
            "      }\n" +
            "    }"), null), 3L, Collections.singletonMap("eggplant", "potato"));
        client().execute(PutComponentTemplateAction.INSTANCE, new PutComponentTemplateAction.Request("my-ct").componentTemplate(ct)).get();

        ComposableIndexTemplate cit = new ComposableIndexTemplate(Collections.singletonList("coleslaw"),
            new Template(null, new CompressedXContent("{\n" +
                "      \"dynamic\": false,\n" +
                "      \"properties\": {\n" +
                "        \"foo\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }"), null), Collections.singletonList("my-ct"),
            4L, 5L, Collections.singletonMap("egg", "bread"));
        client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("my-it").indexTemplate(cit)).get();

        internalCluster().fullRestart();
        ensureGreen();

        ComponentTemplate ct2 = new ComponentTemplate(new Template(null, new CompressedXContent("{\n" +
            "      \"dynamic\": true,\n" +
            "      \"properties\": {\n" +
            "        \"foo\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        }\n" +
            "      }\n" +
            "    }"), null), 3L, Collections.singletonMap("eggplant", "potato"));
        client().execute(PutComponentTemplateAction.INSTANCE,
            new PutComponentTemplateAction.Request("my-ct").componentTemplate(ct2)).get();

        ComposableIndexTemplate cit2 = new ComposableIndexTemplate(Collections.singletonList("coleslaw"),
            new Template(null, new CompressedXContent("{\n" +
                "      \"dynamic\": true,\n" +
                "      \"properties\": {\n" +
                "        \"foo\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }"), null), Collections.singletonList("my-ct"),
            4L, 5L, Collections.singletonMap("egg", "bread"));
        client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("my-it").indexTemplate(cit2)).get();
    }

    public void testUsageOfDataStreamFails() throws IOException {
        // Exception that would happen if a unknown field is provided in a composable template:
        // The thrown exception will be used to compare against the exception that is thrown when providing
        // a composable template with a data stream definition.
        String content = "{\"index_patterns\":[\"logs-*-*\"],\"my_field\":\"bla\"}";
        XContentParser parser =
            XContentHelper.createParser(xContentRegistry(), null, new BytesArray(content), XContentType.JSON);
        Exception expectedException = expectThrows(Exception.class, () -> ComposableIndexTemplate.parse(parser));

        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(List.of("logs-*-*"))
              .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate()).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("my-it").indexTemplate(template)).actionGet());
        Exception actualException = (Exception) e.getCause();
        assertThat(actualException.getMessage(),
            equalTo(expectedException.getMessage().replace("[1:32] ", "").replace("my_field", "data_stream")));
        assertThat(actualException.getMessage(), equalTo("[index_template] unknown field [data_stream]"));
    }
}
