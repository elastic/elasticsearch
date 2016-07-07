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

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.sort.SortBuilders;

/**
 */
public class RandomSortScriptTests extends AbstractSearchScriptTestCase {

    public void testPseudoRandomScript() throws Exception {

        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("name").field("type", "string").endObject()
            .endObject().endObject().endObject()
            .string();

        assertAcked(prepareCreate("test")
            .addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        // Index 100 records (0..99)
        for (int i = 0; i < 100; i++) {
            indexBuilders.add(
                client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "rec " + i)
                        .endObject()));
        }

        indexRandom(true, indexBuilders);

        // Retrieve first 10 records
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setFetchSource("name", null)
            .setSize(10)
            .addSort(SortBuilders.scriptSort(new Script("random", ScriptService.ScriptType.INLINE, "native", MapBuilder.<String, Object>newMapBuilder().put("salt", "1234").map()), "number"))
            .execute().actionGet();

        assertNoFailures(searchResponse);

        // Check that random order was applied
        assertThat(searchResponse.getHits().getAt(0).getSource().get("name"), not(equalTo("rec0")));

        String[] records = new String[10];

        // Store sort order
        for (int i = 0; i < 10; i++) {
            records[i] = searchResponse.getHits().getAt(i).getSource().get("name").toString();
        }

        // Retrieve first 10 records again
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setFetchSource("name", null)
            .setSize(10)
            .addSort(SortBuilders.scriptSort(new Script("random", ScriptService.ScriptType.INLINE, "native", MapBuilder.<String, Object>newMapBuilder().put("salt", "1234").map()), "number"))
            .execute().actionGet();

        assertNoFailures(searchResponse);

        // Verify the same sort order
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).getSource().get("name"), equalTo(records[i]));
        }

        // Retrieve first 10 records without salt
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setFetchSource("name", null)
            .setSize(10)
            .addSort(SortBuilders.scriptSort(new Script("random", ScriptService.ScriptType.INLINE, "native", null), "number"))
            .execute().actionGet();

        assertNoFailures(searchResponse);

        // Verify different sort order
        boolean different = false;
        for (int i = 0; i < 10; i++) {
            if (!records[i].equals(searchResponse.getHits().getAt(i).getSource().get("name"))) {
                different = true;
                break;
            }
        }
        assertThat(different, equalTo(true));

    }
}
