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
package org.elasticsearch.script.mustache;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import org.elasticsearch.test.ESTestCase;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;

/**
 * Figure out how Mustache works for the simplest use case. Leaving in here for now for reference.
 * */
public class MustacheTests extends ESTestCase {
    public void test() {
        HashMap<String, Object> scopes = new HashMap<>();
        scopes.put("boost_val", "0.2");

        String template = "GET _search {\"query\": " + "{\"boosting\": {"
                + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
                + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}"
                + "}}, \"negative_boost\": {{boost_val}} } }}";
        MustacheFactory f = new DefaultMustacheFactory();
        Mustache mustache = f.compile(new StringReader(template), "example");
        StringWriter writer = new StringWriter();
        mustache.execute(writer, scopes);
        writer.flush();
        assertEquals(
                "Mustache templating broken",
                "GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                        + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}}}, \"negative_boost\": 0.2 } }}",
                writer.toString());
    }
}
