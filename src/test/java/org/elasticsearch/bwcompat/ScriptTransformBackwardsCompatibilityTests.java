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

package org.elasticsearch.bwcompat;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class ScriptTransformBackwardsCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testTransformWithNoLangSpecified() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("transform");
        if (getRandom().nextBoolean()) {
            // Single transform
            builder.startObject();
            buildTransformScript(builder);
            builder.endObject();
        } else {
            // Multiple transforms
            int total = between(1, 10);
            int actual = between(0, total - 1);
            builder.startArray();
            for (int s = 0; s < total; s++) {
                builder.startObject();
                if (s == actual) {
                    buildTransformScript(builder);
                } else {
                    builder.field("script", "true");
                }
                builder.endObject();
            }
            builder.endArray();
        }
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("test", builder));

        indexRandom(getRandom().nextBoolean(), client().prepareIndex("test", "test", "notitle").setSource("content", "findme"), client()
                .prepareIndex("test", "test", "badtitle").setSource("content", "findme", "title", "cat"),
                client().prepareIndex("test", "test", "righttitle").setSource("content", "findme", "title", "table"));
        GetResponse response = client().prepareGet("test", "test", "righttitle").get();
        assertExists(response);
        assertThat(response.getSource(), both(hasEntry("content", (Object) "findme")).and(not(hasKey("destination"))));

        response = client().prepareGet("test", "test", "righttitle").setTransformSource(true).get();
        assertExists(response);
        assertThat(response.getSource(), both(hasEntry("destination", (Object) "findme")).and(not(hasKey("content"))));
    }

    private void buildTransformScript(XContentBuilder builder) throws IOException {
        String script = "if (ctx._source['title']?.startsWith('t')) { ctx._source['destination'] = ctx._source[sourceField] }; ctx._source.remove(sourceField);";
        if (getRandom().nextBoolean()) {
            script = script.replace("sourceField", "'content'");
        } else {
            builder.field("params", ImmutableMap.of("sourceField", "content"));
        }
        builder.field("script", script);
    }
}
