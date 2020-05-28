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
package org.elasticsearch.index.mapper;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class LongIndexingDocTests extends ESSingleNodeTestCase {

    public void testLongIndexingOutOfRange() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
              .startObject("properties")
                 .startObject("number")
                   .field("type", "long")
                   .field("ignore_malformed", true)
               .endObject().endObject()
            .endObject().endObject());
        createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        String doc = "{\"number\" : 9223372036854775808}";
        IndexResponse response = client().index(new IndexRequest("test").source(doc, XContentType.JSON)).get();
        assertTrue(response.status() == RestStatus.CREATED);
    }
}
