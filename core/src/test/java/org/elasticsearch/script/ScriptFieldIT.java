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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 3)
public class ScriptFieldIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomScriptPlugin.class);
    }

    static int[] intArray = { Integer.MAX_VALUE, Integer.MIN_VALUE, 3 };
    static long[] longArray = { Long.MAX_VALUE, Long.MIN_VALUE, 9223372036854775807L };
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
                .addScriptField("int", new Script("int", ScriptType.INLINE, "native", null))
                .addScriptField("float", new Script("float", ScriptType.INLINE, "native", null))
                .addScriptField("double", new Script("double", ScriptType.INLINE, "native", null))
                .addScriptField("long", new Script("long", ScriptType.INLINE, "native", null)).execute().actionGet();
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

    public static class IntArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new IntScript();
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "int";
        }
    }

    static class IntScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return intArray;
        }
    }

    public static class LongArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new LongScript();
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "long";
        }
    }

    static class LongScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return longArray;
        }
    }

    public static class FloatArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new FloatScript();
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "float";
        }
    }

    static class FloatScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return floatArray;
        }
    }

    public static class DoubleArrayScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new DoubleScript();
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "double";
        }
    }

    static class DoubleScript extends AbstractSearchScript {
        @Override
        public Object run() {
            return doubleArray;
        }
    }

    public static class CustomScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public List<NativeScriptFactory> getNativeScripts() {
            return Arrays.asList(new IntArrayScriptFactory(), new LongArrayScriptFactory(), new FloatArrayScriptFactory(),
                new DoubleArrayScriptFactory());
        }
    }
}
