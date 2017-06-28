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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

public class IpTermsIT extends AbstractTermsTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends AggregationTestScriptsPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = super.pluginScripts();

            scripts.put("doc['ip'].value", vars -> {
                Map<?, ?> doc = (Map<?,?>) vars.get("doc");
                return doc.get("ip");
            });

            scripts.put("doc['ip'].values", vars -> {
                Map<?, ?> doc = (Map<?,?>) vars.get("doc");
                return ((ScriptDocValues<?>) doc.get("ip")).get(0);
            });

            return scripts;
        }
    }

    public void testScriptValue() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "ip", "type=ip"));
        indexRandom(true,
                client().prepareIndex("index", "type", "1").setSource("ip", "192.168.1.7"),
                client().prepareIndex("index", "type", "2").setSource("ip", "192.168.1.7"),
                client().prepareIndex("index", "type", "3").setSource("ip", "2001:db8::2:1"));

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME,
                "doc['ip'].value", Collections.emptyMap());
        SearchResponse response = client().prepareSearch("index").addAggregation(
                AggregationBuilders.terms("my_terms").script(script).executionHint(randomExecutionHint())).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());

        Terms.Bucket bucket1 = terms.getBuckets().get(0);
        assertEquals(2, bucket1.getDocCount());
        assertEquals("192.168.1.7", bucket1.getKey());
        assertEquals("192.168.1.7", bucket1.getKeyAsString());

        Terms.Bucket bucket2 = terms.getBuckets().get(1);
        assertEquals(1, bucket2.getDocCount());
        assertEquals("2001:db8::2:1", bucket2.getKey());
        assertEquals("2001:db8::2:1", bucket2.getKeyAsString());
    }

    public void testScriptValues() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "ip", "type=ip"));
        indexRandom(true,
                client().prepareIndex("index", "type", "1").setSource("ip", "192.168.1.7"),
                client().prepareIndex("index", "type", "2").setSource("ip", "192.168.1.7"),
                client().prepareIndex("index", "type", "3").setSource("ip", "2001:db8::2:1"));

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME,
                "doc['ip'].values", Collections.emptyMap());
        SearchResponse response = client().prepareSearch("index").addAggregation(
                AggregationBuilders.terms("my_terms").script(script).executionHint(randomExecutionHint())).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());

        Terms.Bucket bucket1 = terms.getBuckets().get(0);
        assertEquals(2, bucket1.getDocCount());
        assertEquals("192.168.1.7", bucket1.getKey());
        assertEquals("192.168.1.7", bucket1.getKeyAsString());

        Terms.Bucket bucket2 = terms.getBuckets().get(1);
        assertEquals(1, bucket2.getDocCount());
        assertEquals("2001:db8::2:1", bucket2.getKey());
        assertEquals("2001:db8::2:1", bucket2.getKeyAsString());
    }
}
