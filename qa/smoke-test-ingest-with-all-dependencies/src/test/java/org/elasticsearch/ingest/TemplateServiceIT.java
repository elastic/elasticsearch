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

package org.elasticsearch.ingest;

import org.elasticsearch.ingest.core.TemplateService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TemplateServiceIT extends AbstractScriptTestCase {

    public void testTemplates() {
        Map<String, Object> model = new HashMap<>();
        model.put("fielda", "value1");
        model.put("fieldb", Collections.singletonMap("fieldc", "value3"));

        TemplateService.Template template = templateService.compile("{{fielda}}/{{fieldb}}/{{fieldb.fieldc}}");
        assertThat(template.execute(model), equalTo("value1/{fieldc=value3}/value3"));
    }

    public void testWrongTemplateUsage() {
        Map<String, Object> model = Collections.emptyMap();
        TemplateService.Template template = templateService.compile("value");
        assertThat(template.execute(model), equalTo("value"));

        template = templateService.compile("value {{");
        assertThat(template.execute(model), equalTo("value {{"));
        template = templateService.compile("value {{abc");
        assertThat(template.execute(model), equalTo("value {{abc"));
        template = templateService.compile("value }}");
        assertThat(template.execute(model), equalTo("value }}"));
        template = templateService.compile("value }} {{");
        assertThat(template.execute(model), equalTo("value }} {{"));
    }

}
