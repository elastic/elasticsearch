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

package org.elasticsearch.messy.tests;

import org.elasticsearch.action.admin.cluster.validate.template.RenderSearchTemplateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.support.FileUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class RenderSearchTemplateTests extends ESIntegTestCase {
    private static final String TEMPLATE_CONTENTS = "{\"size\":\"{{size}}\",\"query\":{\"match\":{\"foo\":\"{{value}}\"}},\"aggs\":{\"objects\":{\"terms\":{\"field\":\"{{value}}\",\"size\":\"{{size}}\"}}}}";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MustachePlugin.class);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        client().preparePutIndexedScript(MustacheScriptEngineService.NAME, "index_template_1", "{ \"template\": " + TEMPLATE_CONTENTS + " }").get();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Path configDir = createTempDir();
        Path scriptsDir = configDir.resolve("scripts");
        try {
            Files.createDirectories(scriptsDir);
            Files.write(scriptsDir.resolve("file_template_1.mustache"), TEMPLATE_CONTENTS.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", configDir).build();
    }

    public void testInlineTemplate() {
        Map<String, Object> params = new HashMap<>();
        params.put("value", "bar");
        params.put("size", 20);
        Template template = new Template(TEMPLATE_CONTENTS, ScriptType.INLINE, MustacheScriptEngineService.NAME, XContentType.JSON, params);
        RenderSearchTemplateResponse response = client().admin().cluster().prepareRenderSearchTemplate().template(template).get();
        assertThat(response, notNullValue());
        BytesReference source = response.source();
        assertThat(source, notNullValue());
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false).v2();
        assertThat(sourceAsMap, notNullValue());
        String expected = TEMPLATE_CONTENTS.replace("{{value}}", "bar").replace("{{size}}", "20");
        Map<String, Object> expectedMap = XContentHelper.convertToMap(new BytesArray(expected), false).v2();
        assertThat(sourceAsMap, equalTo(expectedMap));

        params = new HashMap<>();
        params.put("value", "baz");
        params.put("size", 100);
        template = new Template(TEMPLATE_CONTENTS, ScriptType.INLINE, MustacheScriptEngineService.NAME, XContentType.JSON, params);
        response = client().admin().cluster().prepareRenderSearchTemplate().template(template).get();
        assertThat(response, notNullValue());
        source = response.source();
        assertThat(source, notNullValue());
        sourceAsMap = XContentHelper.convertToMap(source, false).v2();
        expected = TEMPLATE_CONTENTS.replace("{{value}}", "baz").replace("{{size}}", "100");
        expectedMap = XContentHelper.convertToMap(new BytesArray(expected), false).v2();
        assertThat(sourceAsMap, equalTo(expectedMap));
    }

    public void testIndexedTemplate() {
        Map<String, Object> params = new HashMap<>();
        params.put("value", "bar");
        params.put("size", 20);
        Template template = new Template("index_template_1", ScriptType.INDEXED, MustacheScriptEngineService.NAME, XContentType.JSON, params);
        RenderSearchTemplateResponse response = client().admin().cluster().prepareRenderSearchTemplate().template(template).get();
        assertThat(response, notNullValue());
        BytesReference source = response.source();
        assertThat(source, notNullValue());
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false).v2();
        assertThat(sourceAsMap, notNullValue());
        String expected = TEMPLATE_CONTENTS.replace("{{value}}", "bar").replace("{{size}}", "20");
        Map<String, Object> expectedMap = XContentHelper.convertToMap(new BytesArray(expected), false).v2();
        assertThat(sourceAsMap, equalTo(expectedMap));

        params = new HashMap<>();
        params.put("value", "baz");
        params.put("size", 100);
        template = new Template("index_template_1", ScriptType.INDEXED, MustacheScriptEngineService.NAME, XContentType.JSON, params);
        response = client().admin().cluster().prepareRenderSearchTemplate().template(template).get();
        assertThat(response, notNullValue());
        source = response.source();
        assertThat(source, notNullValue());
        sourceAsMap = XContentHelper.convertToMap(source, false).v2();
        expected = TEMPLATE_CONTENTS.replace("{{value}}", "baz").replace("{{size}}", "100");
        expectedMap = XContentHelper.convertToMap(new BytesArray(expected), false).v2();
        assertThat(sourceAsMap, equalTo(expectedMap));
    }

    public void testFileTemplate() {
        Map<String, Object> params = new HashMap<>();
        params.put("value", "bar");
        params.put("size", 20);
        Template template = new Template("file_template_1", ScriptType.FILE, MustacheScriptEngineService.NAME, XContentType.JSON, params);
        RenderSearchTemplateResponse response = client().admin().cluster().prepareRenderSearchTemplate().template(template).get();
        assertThat(response, notNullValue());
        BytesReference source = response.source();
        assertThat(source, notNullValue());
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false).v2();
        assertThat(sourceAsMap, notNullValue());
        String expected = TEMPLATE_CONTENTS.replace("{{value}}", "bar").replace("{{size}}", "20");
        Map<String, Object> expectedMap = XContentHelper.convertToMap(new BytesArray(expected), false).v2();
        assertThat(sourceAsMap, equalTo(expectedMap));

        params = new HashMap<>();
        params.put("value", "baz");
        params.put("size", 100);
        template = new Template("file_template_1", ScriptType.FILE, MustacheScriptEngineService.NAME, XContentType.JSON, params);
        response = client().admin().cluster().prepareRenderSearchTemplate().template(template).get();
        assertThat(response, notNullValue());
        source = response.source();
        assertThat(source, notNullValue());
        sourceAsMap = XContentHelper.convertToMap(source, false).v2();
        expected = TEMPLATE_CONTENTS.replace("{{value}}", "baz").replace("{{size}}", "100");
        expectedMap = XContentHelper.convertToMap(new BytesArray(expected), false).v2();
        assertThat(sourceAsMap, equalTo(expectedMap));
    }
}
