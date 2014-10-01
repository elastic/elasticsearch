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

package org.elasticsearch.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 3)
public class ScriptFieldTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("plugin.types", CustomScriptPlugin.class.getName()).build();
    }

    static int[] intArray = { Integer.MAX_VALUE, Integer.MIN_VALUE, 3 };
    static long[] longArray = { Long.MAX_VALUE, Long.MIN_VALUE, 9223372036854775807l };
    static float[] floatArray = { Float.MAX_VALUE, Float.MIN_VALUE, 3.3f };
    static double[] doubleArray = { Double.MAX_VALUE, Double.MIN_VALUE, 3.3d };

    public void testNativeScript() throws InterruptedException, ExecutionException {

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("text", "doc1"), client()
                .prepareIndex("test", "type1", "2").setSource("text", "doc2"),
                client().prepareIndex("test", "type1", "3").setSource("text", "doc3"), client().prepareIndex("test", "type1", "4")
                        .setSource("text", "doc4"), client().prepareIndex("test", "type1", "5").setSource("text", "doc5"), client()
                        .prepareIndex("test", "type1", "6").setSource("text", "doc6"));

        client().admin().indices().prepareFlush("test").execute().actionGet();
        SearchResponse sr = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .addScriptField("int", "native", "int", null).addScriptField("float", "native", "float", null)
                .addScriptField("double", "native", "double", null).addScriptField("long", "native", "long", null).execute().actionGet();
        assertThat(sr.getHits().hits().length, equalTo(6));
        for (SearchHit hit : sr.getHits().getHits()) {
            Object result = hit.getFields().get("int").getValues().get(0);
            assertThat(result, equalTo((Object) intArray));
            result = hit.getFields().get("long").getValues().get(0);
            assertThat(result, equalTo((Object) longArray));
            result = hit.getFields().get("float").getValues().get(0);
            assertThat(result, equalTo((Object) floatArray));
            result = hit.getFields().get("double").getValues().get(0);
            assertThat(result, equalTo((Object) doubleArray));
        }
    }

    static class IntArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new IntScript();
        }
    }

    static class IntScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return intArray;
        }
    }

    static class LongArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new LongScript();
        }
    }

    static class LongScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return longArray;
        }
    }

    static class FloatArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new FloatScript();
        }
    }

    static class FloatScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return floatArray;
        }
    }

    static class DoubleArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new DoubleScript();
        }
    }

    static class DoubleScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return doubleArray;
        }
    }

    public static class CustomScriptPlugin extends AbstractPlugin {

        @Override
        public String name() {
            return "custom_script";
        }

        @Override
        public String description() {
            return "script ";
        }

        public void onModule(ScriptModule scriptModule) {
            scriptModule.registerScript("int", IntArrayScriptFactory.class);
            scriptModule.registerScript("long", LongArrayScriptFactory.class);
            scriptModule.registerScript("float", FloatArrayScriptFactory.class);
            scriptModule.registerScript("double", DoubleArrayScriptFactory.class);
        }

    }
}
