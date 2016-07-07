/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.scriptedMetric;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ScriptedMetricAggsScriptTests extends AbstractSearchScriptTestCase {

    @SuppressWarnings("unchecked")
    public void testScriptedMetricAggs() throws Exception {

        // Create a new lookup index
        String stockMapping = XContentFactory.jsonBuilder().startObject().startObject("stock")
                .startObject("properties")
                .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("amount").field("type", "long").endObject()
                .endObject().endObject().endObject()
                .string();

        assertAcked(prepareCreate("transactions")
                .addMapping("stock", stockMapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index stock records:
        indexBuilders.add(client().prepareIndex("transactions", "stock", "1").setSource("type", "sale", "amount", 80));
        indexBuilders.add(client().prepareIndex("transactions", "stock", "2").setSource("type", "cost", "amount", 10));
        indexBuilders.add(client().prepareIndex("transactions", "stock", "3").setSource("type", "cost", "amount", 30));
        indexBuilders.add(client().prepareIndex("transactions", "stock", "4").setSource("type", "sale", "amount", 130));

        indexRandom(true, indexBuilders);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(matchAllQuery())
                .setSize(0)
                .addAggregation(scriptedMetric("profit")
                        .initScript(new Script("stockaggs_init", ScriptService.ScriptType.INLINE, "native", null))
                        .mapScript(new Script("stockaggs_map", ScriptService.ScriptType.INLINE, "native", null))
                        .combineScript(new Script("stockaggs_combine", ScriptService.ScriptType.INLINE, "native", null))
                        .reduceScript(new Script("stockaggs_reduce", ScriptService.ScriptType.INLINE, "native", null)))
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 4 hits - we are running aggs on everything
        assertHitCount(searchResponse, 4);

        // The profit should be 170
        assertThat((long) searchResponse.getAggregations().get("profit").getProperty("value"), equalTo(170L));
    }


}
