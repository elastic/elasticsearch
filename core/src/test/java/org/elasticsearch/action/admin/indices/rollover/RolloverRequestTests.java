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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class RolloverRequestTests extends ESTestCase {

    public void testConditionsParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAsciiOfLength(10), randomAsciiOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("conditions")
                .field("max_age", "10d")
                .field("max_docs", 100)
            .endObject()
            .endObject();
        request.source(builder.bytes());
        Set<Condition> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(2));
        for (Condition condition : conditions) {
            if (condition instanceof MaxAgeCondition) {
                MaxAgeCondition maxAgeCondition = (MaxAgeCondition) condition;
                assertThat(maxAgeCondition.value.getMillis(), equalTo(TimeValue.timeValueHours(24 * 10).getMillis()));
            } else if (condition instanceof MaxDocsCondition) {
                MaxDocsCondition maxDocsCondition = (MaxDocsCondition) condition;
                assertThat(maxDocsCondition.value, equalTo(100L));
            } else {
                fail("unexpected condition " + condition);
            }
        }
    }

    public void testParsingWithIndexSettings() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAsciiOfLength(10), randomAsciiOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("conditions")
                .field("max_age", "10d")
                .field("max_docs", 100)
            .endObject()
            .startObject("mappings")
                .startObject("type1")
                    .startObject("properties")
                        .startObject("field1")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .startObject("settings")
                .field("number_of_shards", 10)
            .endObject()
            .startObject("aliases")
                .startObject("alias1").endObject()
            .endObject()
            .endObject();
        request.source(builder.bytes());
        Set<Condition> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(2));
        assertThat(request.getCreateIndexRequest().mappings().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().aliases().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().settings().getAsInt("number_of_shards", 0), equalTo(10));
    }
}
