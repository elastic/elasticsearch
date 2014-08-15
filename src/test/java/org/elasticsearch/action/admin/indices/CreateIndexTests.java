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
package org.elasticsearch.action.admin.indices;

import java.util.HashMap;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class CreateIndexTests extends ElasticsearchIntegrationTest {
    public void testDoubleAddMapping() throws Exception {
        try {
            prepareCreate("test")
                .addMapping("type1", "date", "type=date")
                .addMapping("type1", "num", "type=integer");    
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                .addMapping("type1", new HashMap<String,Object>())
                .addMapping("type1", new HashMap<String,Object>());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                .addMapping("type1", jsonBuilder())
                .addMapping("type1", jsonBuilder());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
            System.out.println("HERE");
        }
    }
}
