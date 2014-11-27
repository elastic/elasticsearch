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

package org.elasticsearch.index.mapper.all;

import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class AllMapperOnCusterTests extends ElasticsearchIntegrationTest {

    private static final String INDEX = "index";
    private static final String TYPE = "type";

    @Test
    public void test_doc_valuesInvalidMapping() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("mappings").startObject(TYPE).startObject("_all").startObject("fielddata").field("format", "doc_values").endObject().endObject().endObject().endObject().endObject().string();
        try {
            prepareCreate(INDEX).setSource(mapping).get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("[_all] is always tokenized and cannot have doc values"));
        }
    }
}
