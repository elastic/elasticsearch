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

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;

public class InternalTemplateService implements TemplateService {

    private final ScriptService scriptService;

    InternalTemplateService(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    @Override
    public Template compile(String template) {
        int mustacheStart = template.indexOf("{{");
        int mustacheEnd = template.indexOf("}}");
        if (mustacheStart != -1 && mustacheEnd != -1 && mustacheStart < mustacheEnd) {
            Script script = new Script(ScriptType.INLINE, "mustache", template, Collections.emptyMap());
            TemplateScript.Factory compiledTemplate = scriptService.compile(script, TemplateScript.CONTEXT);
            return new Template() {
                @Override
                public String execute(Map<String, Object> model) {
                    return compiledTemplate.newInstance(model).execute();
                }

                @Override
                public String getKey() {
                    return template;
                }
            };
        } else {
            return new StringTemplate(template);
        }
    }

    class StringTemplate implements Template {

        private final String value;

        StringTemplate(String value) {
            this.value = value;
        }

        @Override
        public String execute(Map<String, Object> model) {
            return value;
        }

        @Override
        public String getKey() {
            return value;
        }
    }
}
