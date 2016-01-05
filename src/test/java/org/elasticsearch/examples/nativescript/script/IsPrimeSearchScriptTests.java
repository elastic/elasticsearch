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
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class IsPrimeSearchScriptTests extends AbstractSearchScriptTestCase {

    public static int[] PRIMES_10 = new int[]{2, 3, 5, 7, 11, 13, 17, 19, 23, 29};

    public void testIsPrimeScript() throws Exception {

        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("name").field("type", "string").endObject()
            .startObject("number").field("type", "integer").endObject()
            .endObject().endObject().endObject()
            .string();

        assertAcked(prepareCreate("test")
            .addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index 100 records (0..99)
        for (int i = 0; i < 100; i++) {
            indexBuilders.add(
                client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "rec " + i)
                        .field("number", i)
                        .endObject()));
        }
        // Index a few records with empty number
        for (int i = 100; i < 105; i++) {
            indexBuilders.add(
                client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "rec " + i)
                        .endObject()));
        }

        indexRandom(true, indexBuilders);

        Map<String, Object> params = new HashMap<>();
        params.put("field", "number");
        // Retrieve first 10 prime records
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(scriptQuery(new Script("is_prime", ScriptService.ScriptType.INLINE, "native", params)))
            .setFetchSource("name", null)
            .setSize(10)
            .addSort("number", SortOrder.ASC)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 25 prime numbers between 0 and 100
        assertHitCount(searchResponse, 25);

        // Verify that they are indeed prime numbers
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).getSource().get("name"), equalTo("rec " + PRIMES_10[i]));
        }

        params = new HashMap<>();
        params.put("field", "number");
        params.put("certainty", 0);
        // Check certainty parameter - with certainty == 0, it should return all numbers, but only if numbers are present
        searchResponse = client().prepareSearch("test")
            .setQuery(scriptQuery(new Script("is_prime", ScriptService.ScriptType.INLINE, "native", params)))
            .setFetchSource("name", null)
            .setSize(10)
            .addSort("number", SortOrder.ASC)
            .execute().actionGet();
        assertNoFailures(searchResponse);
        // With certainty 0 no check is done so it should return all numbers
        assertHitCount(searchResponse, 100);

    }

}
