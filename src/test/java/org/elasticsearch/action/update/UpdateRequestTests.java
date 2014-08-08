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

package org.elasticsearch.action.update;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class UpdateRequestTests extends ElasticsearchTestCase {

    @Test
    public void testUpdateRequest() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "type", "1");
        // simple script
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));

        // script with params
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .startObject("params").field("param1", "value1").endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        // script with params and upsert
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        Map<String, Object> upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        // script with doc
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("doc").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        Map<String, Object> doc = request.doc().sourceAsMap();
        assertThat(doc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) doc.get("compound")).get("field2").toString(), equalTo("value2"));
    }
}
