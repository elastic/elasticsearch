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

package org.elasticsearch.ingest.processor.gsub;

import org.elasticsearch.ingest.processor.join.JoinProcessor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class GsubProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws IOException {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<Map<String, String>> expressions = new ArrayList<>();
        Map<String, String> expression = new HashMap<>();
        expression.put("field", "field1");
        expression.put("pattern", "\\.");
        expression.put("replacement", "-");
        expressions.add(expression);
        config.put("expressions", expressions);
        GsubProcessor gsubProcessor = factory.create(config);
        assertThat(gsubProcessor.getGsubExpressions().size(), equalTo(1));
        GsubExpression gsubExpression = gsubProcessor.getGsubExpressions().get(0);
        assertThat(gsubExpression.getFieldName(), equalTo("field1"));
        assertThat(gsubExpression.getPattern().toString(), equalTo("\\."));
        assertThat(gsubExpression.getReplacement(), equalTo("-"));
    }

    public void testCreateMissingExpressions() throws IOException {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("required property [expressions] is missing"));
        }
    }

    public void testCreateNoFieldPresent() throws IOException {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<Map<String, String>> expressions = new ArrayList<>();
        Map<String, String> expression = new HashMap<>();
        expression.put("pattern", "\\.");
        expression.put("replacement", "-");
        expressions.add(expression);
        config.put("expressions", expressions);
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("no [field] specified for gsub expression"));
        }
    }

    public void testCreateNoPatternPresent() throws IOException {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<Map<String, String>> expressions = new ArrayList<>();
        Map<String, String> expression = new HashMap<>();
        expression.put("field", "field1");
        expression.put("replacement", "-");
        expressions.add(expression);
        config.put("expressions", expressions);
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("no [pattern] specified for gsub expression"));
        }
    }

    public void testCreateNoReplacementPresent() throws IOException {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<Map<String, String>> expressions = new ArrayList<>();
        Map<String, String> expression = new HashMap<>();
        expression.put("field", "field1");
        expression.put("pattern", "\\.");
        expressions.add(expression);
        config.put("expressions", expressions);
        try {
            factory.create(config);
            fail("factory create should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("no [replacement] specified for gsub expression"));
        }
    }
}
