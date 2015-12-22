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

package org.elasticsearch.index.mapper.boost;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CustomBoostMappingTests extends ESSingleNodeTestCase {
    public void testCustomBoostValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("s_field").field("type", "string").endObject()
                .startObject("l_field").field("type", "long").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("i_field").field("type", "integer").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("sh_field").field("type", "short").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("b_field").field("type", "byte").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("d_field").field("type", "double").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("f_field").field("type", "float").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("date_field").field("type", "date").startObject("norms").field("enabled", true).endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .startObject("s_field").field("value", "s_value").field("boost", 2.0f).endObject()
                .startObject("l_field").field("value", 1l).field("boost", 3.0f).endObject()
                .startObject("i_field").field("value", 1).field("boost", 4.0f).endObject()
                .startObject("sh_field").field("value", 1).field("boost", 5.0f).endObject()
                .startObject("b_field").field("value", 1).field("boost", 6.0f).endObject()
                .startObject("d_field").field("value", 1).field("boost", 7.0f).endObject()
                .startObject("f_field").field("value", 1).field("boost", 8.0f).endObject()
                .startObject("date_field").field("value", "20100101").field("boost", 9.0f).endObject()
                .endObject().bytes());

        assertThat(doc.rootDoc().getField("s_field").boost(), equalTo(2.0f));
        assertThat(doc.rootDoc().getField("l_field").boost(), equalTo(3.0f));
        assertThat(doc.rootDoc().getField("i_field").boost(), equalTo(4.0f));
        assertThat(doc.rootDoc().getField("sh_field").boost(), equalTo(5.0f));
        assertThat(doc.rootDoc().getField("b_field").boost(), equalTo(6.0f));
        assertThat(doc.rootDoc().getField("d_field").boost(), equalTo(7.0f));
        assertThat(doc.rootDoc().getField("f_field").boost(), equalTo(8.0f));
        assertThat(doc.rootDoc().getField("date_field").boost(), equalTo(9.0f));
    }
}