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
package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test building and serialising a template search request.
 * */
public class TemplateQueryBuilderTests extends ESTestCase {

    @Test
    public void testJSONGeneration() throws IOException {
        Map<String, Object> vars = new HashMap<>();
        vars.put("template", "filled");
        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                new Template("I am a $template string", ScriptType.INLINE, null, null, vars));
        XContentBuilder content = XContentFactory.jsonBuilder();
        content.startObject();
        builder.doXContent(content, null);
        content.endObject();
        content.close();
        assertEquals("{\"template\":{\"inline\":\"I am a $template string\",\"params\":{\"template\":\"filled\"}}}", content.string());
    }

}
