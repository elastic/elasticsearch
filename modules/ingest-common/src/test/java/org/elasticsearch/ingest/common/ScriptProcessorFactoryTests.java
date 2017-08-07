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

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptProcessorFactoryTests extends ESTestCase {

    private ScriptProcessor.Factory factory;
    private static final Map<String, String> ingestScriptParamToType;
    static {
        Map<String, String> map = new HashMap<>();
        map.put("id", "stored");
        map.put("source", "inline");
        ingestScriptParamToType = Collections.unmodifiableMap(map);
    }

    @Before
    public void init() {
        factory = new ScriptProcessor.Factory(mock(ScriptService.class));
    }

    public void testFactoryValidationWithDefaultLang() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "source");
        configMap.put(randomType, "foo");
        ScriptProcessor processor = factory.create(null, randomAlphaOfLength(10), configMap);
        assertThat(processor.getScript().getLang(), equalTo(randomType.equals("id") ? null : Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType().toString(), equalTo(ingestScriptParamToType.get(randomType)));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
    }

    public void testFactoryValidationWithParams() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "source");
        Map<String, Object> randomParams = Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10));
        configMap.put(randomType, "foo");
        configMap.put("params", randomParams);
        ScriptProcessor processor = factory.create(null, randomAlphaOfLength(10), configMap);
        assertThat(processor.getScript().getLang(), equalTo(randomType.equals("id") ? null : Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType().toString(), equalTo(ingestScriptParamToType.get(randomType)));
        assertThat(processor.getScript().getParams(), equalTo(randomParams));
    }

    public void testFactoryValidationForMultipleScriptingTypes() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("id", "foo");
        configMap.put("source", "bar");
        configMap.put("lang", "mockscript");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, randomAlphaOfLength(10), configMap));
        assertThat(exception.getMessage(), is("Only one of [id] or [source] may be configured"));
    }

    public void testFactoryValidationAtLeastOneScriptingType() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("lang", "mockscript");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, randomAlphaOfLength(10), configMap));

        assertThat(exception.getMessage(), is("Need [id] or [source] parameter to refer to scripts"));
    }

    public void testInlineBackcompat() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("inline", "code");

        factory.create(null, randomAlphaOfLength(10), configMap);
        assertWarnings("Specifying script source with [inline] is deprecated, use [source] instead.");
    }

    public void testFactoryInvalidateWithInvalidCompiledScript() throws Exception {
        String randomType = randomFrom("source", "id");
        ScriptService mockedScriptService = mock(ScriptService.class);
        ScriptException thrownException = new ScriptException("compile-time exception", new RuntimeException(),
            Collections.emptyList(), "script", "mockscript");
        when(mockedScriptService.compile(any(), any())).thenThrow(thrownException);
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("lang", "mockscript");
        configMap.put(randomType, "my_script");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, randomAlphaOfLength(10), configMap));

        assertThat(exception.getMessage(), is("compile-time exception"));
    }
}
