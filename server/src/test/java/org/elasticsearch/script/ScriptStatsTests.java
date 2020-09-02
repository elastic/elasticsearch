/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ScriptStatsTests extends ESTestCase {
    public void testXContent() throws IOException {
        List<ScriptContextStats> contextStats = List.of(
            new ScriptContextStats("contextB", 100, 201, 302),
            new ScriptContextStats("contextA", 1000, 2010, 3020)
        );
        ScriptStats stats = new ScriptStats(contextStats);
        final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = "{\n" +
            "  \"script\" : {\n" +
            "    \"compilations\" : 1100,\n" +
            "    \"cache_evictions\" : 2211,\n" +
            "    \"compilation_limit_triggered\" : 3322,\n" +
            "    \"contexts\" : [\n" +
            "      {\n" +
            "        \"context\" : \"contextA\",\n" +
            "        \"compilations\" : 1000,\n" +
            "        \"cache_evictions\" : 2010,\n" +
            "        \"compilation_limit_triggered\" : 3020\n" +
            "      },\n" +
            "      {\n" +
            "        \"context\" : \"contextB\",\n" +
            "        \"compilations\" : 100,\n" +
            "        \"cache_evictions\" : 201,\n" +
            "        \"compilation_limit_triggered\" : 302\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";
        assertThat(Strings.toString(builder), equalTo(expected));
    }
}
