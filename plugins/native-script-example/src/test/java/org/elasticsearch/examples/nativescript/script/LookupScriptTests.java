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

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.sort.SortOrder;

/**
 */
public class LookupScriptTests extends AbstractSearchScriptTestCase {

    @SuppressWarnings("unchecked")
    public void testLookup() throws Exception {

        // Create a new lookup index
        String lookup_mapping = XContentFactory.jsonBuilder().startObject().startObject("state")
            .startObject("properties")
            .startObject("name").field("type", "string").endObject()
            .startObject("capital").field("type", "string").endObject()
            .startObject("nickname").field("type", "string").endObject()
            .endObject().endObject().endObject()
            .string();

        assertAcked(prepareCreate("lookup")
            .addMapping("state", lookup_mapping));

        // Create a new test index
        String test_mapping = XContentFactory.jsonBuilder().startObject().startObject("city")
            .startObject("properties")
            .startObject("city").field("type", "string").endObject()
            .startObject("state").field("type", "string").field("index", "not_analyzed").endObject()
            .startObject("population").field("type", "integer").endObject()
            .endObject().endObject().endObject()
            .string();

        assertAcked(prepareCreate("test")
            .addMapping("city", test_mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index Lookup records:
        indexBuilders.add(client().prepareIndex("lookup", "state", "CT").setSource("name", "Connecticut", "capital", "Hartford", "nickname", "Constitution State"));
        indexBuilders.add(client().prepareIndex("lookup", "state", "ME").setSource("name", "Maine", "capital", "Augusta", "nickname", "Lumber State"));
        indexBuilders.add(client().prepareIndex("lookup", "state", "MA").setSource("name", "Massachusetts", "capital", "Boston", "nickname", "Bay State"));
        indexBuilders.add(client().prepareIndex("lookup", "state", "NH").setSource("name", "New Hampshire", "capital", "Concord", "nickname", "Granite State"));
        indexBuilders.add(client().prepareIndex("lookup", "state", "RI").setSource("name", "Rhode Island", "capital", "Providence", "nickname", "Little Rhody"));
        indexBuilders.add(client().prepareIndex("lookup", "state", "VT").setSource("name", "Vermont", "capital", "Montpelier", "nickname", "Green Mountain State"));

        // Index main records
        indexBuilders.add(client().prepareIndex("test", "city", "1").setSource("city", "Cambridge", "state", "MA", "population", 105162));
        indexBuilders.add(client().prepareIndex("test", "city", "2").setSource("city", "South Burlington", "state", "VT", "population", 17904));
        indexBuilders.add(client().prepareIndex("test", "city", "3").setSource("city", "South Portland", "state", "ME", "population", 25002));
        indexBuilders.add(client().prepareIndex("test", "city", "4").setSource("city", "Essex", "state", "VT", "population", 19587));
        indexBuilders.add(client().prepareIndex("test", "city", "5").setSource("city", "Portland", "state", "ME", "population", 66194));
        indexBuilders.add(client().prepareIndex("test", "city", "6").setSource("city", "Burlington", "state", "VT", "population", 42417));
        indexBuilders.add(client().prepareIndex("test", "city", "7").setSource("city", "Stamford", "state", "CT", "population", 122643));
        indexBuilders.add(client().prepareIndex("test", "city", "8").setSource("city", "Colchester", "state", "VT", "population", 17067));
        indexBuilders.add(client().prepareIndex("test", "city", "9").setSource("city", "Concord", "state", "NH", "population", 42695));
        indexBuilders.add(client().prepareIndex("test", "city", "10").setSource("city", "Boston", "state", "MA", "population", 617594));

        indexRandom(true, indexBuilders);

        // Script parameters
        Map<String, Object> params = MapBuilder.<String, Object>newMapBuilder()
            .put("lookup_index", "lookup")
            .put("lookup_type", "state")
            .put("field", "state")
            .map();


        // Find smallest city with word
        SearchResponse searchResponse = client().prepareSearch("test")
            .setTypes("city")
            .setQuery(matchQuery("city", "south burlington"))
            .setFetchSource(true)
            .addScriptField("state_info", new Script("lookup", ScriptService.ScriptType.INLINE, "native", params))
            .setSize(10)
            .addSort("population", SortOrder.DESC)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 3 cities
        assertHitCount(searchResponse, 3);

        assertThat(searchResponse.getHits().getAt(0).getSource().get("city"), equalTo("Burlington"));
        assertThat(((Map<String, Object>) searchResponse.getHits().getAt(0).field("state_info").getValue()).get("name").toString(), equalTo("Vermont"));

        assertThat(searchResponse.getHits().getAt(1).getSource().get("city"), equalTo("South Portland"));
        assertThat(((Map<String, Object>) searchResponse.getHits().getAt(1).field("state_info").getValue()).get("name").toString(), equalTo("Maine"));

        assertThat(searchResponse.getHits().getAt(2).getSource().get("city"), equalTo("South Burlington"));
        assertThat(((Map<String, Object>) searchResponse.getHits().getAt(2).field("state_info").getValue()).get("name").toString(), equalTo("Vermont"));
    }


}
