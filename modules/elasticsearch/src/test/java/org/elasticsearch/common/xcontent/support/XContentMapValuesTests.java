/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 */
@Test
public class XContentMapValuesTests {

    @SuppressWarnings({"unchecked"}) @Test public void testExtractValue() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("test", "value")
                .endObject();

        Map<String, Object> map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractValue("test", map).toString(), equalTo("value"));
        assertThat(XContentMapValues.extractValue("test.me", map), nullValue());
        assertThat(XContentMapValues.extractValue("something.else.2", map), nullValue());

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1").startObject("path2").field("test", "value").endObject().endObject()
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractValue("path1.path2.test", map).toString(), equalTo("value"));
        assertThat(XContentMapValues.extractValue("path1.path2.test_me", map), nullValue());
        assertThat(XContentMapValues.extractValue("path1.non_path2.test", map), nullValue());

        Object extValue = XContentMapValues.extractValue("path1.path2", map);
        assertThat(extValue, instanceOf(Map.class));
        Map<String, Object> extMapValue = (Map<String, Object>) extValue;
        assertThat(extMapValue, hasEntry("test", (Object) "value"));

        extValue = XContentMapValues.extractValue("path1", map);
        assertThat(extValue, instanceOf(Map.class));
        extMapValue = (Map<String, Object>) extValue;
        assertThat(extMapValue.containsKey("path2"), equalTo(true));

        // lists
        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1").field("test", "value1", "value2").endObject()
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();

        extValue = XContentMapValues.extractValue("path1.test", map);
        assertThat(extValue, instanceOf(List.class));

        List extListValue = (List) extValue;
        assertThat(extListValue.size(), equalTo(2));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1")
                .startArray("path2")
                .startObject().field("test", "value1").endObject()
                .startObject().field("test", "value2").endObject()
                .endArray()
                .endObject()
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();

        extValue = XContentMapValues.extractValue("path1.path2.test", map);
        assertThat(extValue, instanceOf(List.class));

        extListValue = (List) extValue;
        assertThat(extListValue.size(), equalTo(2));
        assertThat(extListValue.get(0).toString(), equalTo("value1"));
        assertThat(extListValue.get(1).toString(), equalTo("value2"));
    }
}
