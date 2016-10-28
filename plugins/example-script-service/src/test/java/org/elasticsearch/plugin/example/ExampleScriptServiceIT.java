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

package org.elasticsearch.plugin.example;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

public class ExampleScriptServiceIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ExampleScriptServicePlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(ExampleScriptServicePlugin.class);
    }


    public void testIsHolidayScript() throws Exception {
        createIndex("index");
        client().prepareIndex("index", "event", "1").setSource("event", "Not a holiday", "date", "2016-10-19").execute().actionGet();
        client().prepareIndex("index", "event", "2").setSource("event", "New Year Day", "date", "2017-01-01").execute().actionGet();
        client().prepareIndex("index", "event", "3").setSource("event", "Thanksgiving 2017", "date", "2017-11-23").execute().actionGet();
        client().prepareIndex("index", "event", "4").setSource("event", "Christmas", "date", "2016-12-25").execute().actionGet();
        client().prepareIndex("index", "event", "5").setSource("event", "Not a holiday", "date", "2016-10-10").execute().actionGet();
        refresh();
        Map<String, Object> params = new HashMap<>();
        params.put("field", "date");

        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("is_holiday",
            new Script("is_holiday", ScriptType.INLINE, ExampleScriptService.NAME, params))
            .addStoredField("_source").get();
        assertSearchResponse(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String name = (String) hit.getSource().get("event");
            boolean isAHoliday = ((Boolean) (hit.field("is_holiday").values().get(0)));
            assertEquals(name, name.equals("Not a holiday"), isAHoliday == false);
        }

        params.put("country", "NETHERLANDS"); // try testing with  Dutch holidays
        searchResponse = client().prepareSearch("index").addScriptField("is_holiday",
            new Script("is_holiday", ScriptType.INLINE, ExampleScriptService.NAME, params))
            .addStoredField("_source").get();
        assertSearchResponse(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String name = (String) hit.getSource().get("event");
            boolean isAHoliday = ((Boolean) (hit.field("is_holiday").values().get(0)));
            assertEquals(name, name.equals("Not a holiday") || name.equals("Thanksgiving 2017"), isAHoliday == false);
        }
    }

}
