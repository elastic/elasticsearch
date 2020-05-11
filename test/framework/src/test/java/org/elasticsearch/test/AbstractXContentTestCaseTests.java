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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AbstractXContentTestCaseTests extends ESTestCase {

    public void testInsertRandomFieldsAndShuffle() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("field", 1);
        }
        builder.endObject();
        BytesReference insertRandomFieldsAndShuffle = RandomizedContext.current().runWithPrivateRandomness(1,
                () -> AbstractXContentTestCase.insertRandomFieldsAndShuffle(
                        BytesReference.bytes(builder),
                        XContentType.JSON,
                        true,
                        new String[] {},
                        null,
                        this::createParser));
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), insertRandomFieldsAndShuffle)) {
            Map<String, Object> mapOrdered = parser.mapOrdered();
            assertThat(mapOrdered.size(), equalTo(2));
            assertThat(mapOrdered.keySet().iterator().next(), not(equalTo("field")));
        }
    }
}
