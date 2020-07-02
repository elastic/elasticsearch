/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
}
