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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ScriptProcessorFactoryTests extends ESTestCase {

    private ScriptProcessor.Factory factory;

    @Before
    public void init() {
        factory = new ScriptProcessor.Factory(mock(ScriptService.class), mock(ClusterService.class));
    }


    public void testFactoryValidationForMultipleScriptingTypes() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "inline", "file");
        String otherRandomType = randomFrom("id", "inline", "file");
        while (randomType.equals(otherRandomType)) {
            otherRandomType = randomFrom("id", "inline", "file");
        }

        configMap.put(randomType, "foo");
        configMap.put(otherRandomType, "bar");
        configMap.put("field", "my_field");
        configMap.put("lang", "mockscript");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.doCreate(randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("[null] Only one of [file], [id], or [inline] may be configured"));
    }

    public void testFactoryValidationAtLeastOneScriptingType() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("field", "my_field");
        configMap.put("lang", "mockscript");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.doCreate(randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("[null] Need [file], [id], or [inline] parameter to refer to scripts"));
    }
}
