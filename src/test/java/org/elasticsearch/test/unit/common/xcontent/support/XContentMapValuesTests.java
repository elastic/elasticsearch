/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.common.xcontent.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
@Test
public class XContentMapValuesTests {

    @Test
    public void testFilter() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("test1", "value1")
                .field("test2", "value2")
                .endObject();

        Map<String, Object> source = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        Map<String, Object> filter = XContentMapValues.filter(source, new String[]{"test1"}, Strings.EMPTY_ARRAY);
        assertThat(filter.size(), equalTo(1));
        assertThat(filter.get("test1").toString(), equalTo("value1"));

        filter = XContentMapValues.filter(source, new String[]{"test*"}, Strings.EMPTY_ARRAY);
        assertThat(filter.size(), equalTo(2));
        assertThat(filter.get("test1").toString(), equalTo("value1"));
        assertThat(filter.get("test2").toString(), equalTo("value2"));

        filter = XContentMapValues.filter(source, Strings.EMPTY_ARRAY, new String[]{"test1"});
        assertThat(filter.size(), equalTo(1));
        assertThat(filter.get("test2").toString(), equalTo("value2"));

        // more complex object...
        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1")
                .startArray("path2")
                .startObject().field("test", "value1").endObject()
                .startObject().field("test", "value2").endObject()
                .endArray()
                .endObject()
                .field("test1", "value1")
                .endObject();

        source = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        filter = XContentMapValues.filter(source, new String[]{"path1"}, Strings.EMPTY_ARRAY);
        assertThat(filter.size(), equalTo(0));

        filter = XContentMapValues.filter(source, new String[]{"path1*"}, Strings.EMPTY_ARRAY);
        assertThat(filter.get("path1"), equalTo(source.get("path1")));
        assertThat(filter.containsKey("test1"), equalTo(false));

        filter = XContentMapValues.filter(source, new String[]{"test1*"}, Strings.EMPTY_ARRAY);
        assertThat(filter.get("test1"), equalTo(source.get("test1")));
        assertThat(filter.containsKey("path1"), equalTo(false));

        filter = XContentMapValues.filter(source, new String[]{"path1.path2.*"}, Strings.EMPTY_ARRAY);
        assertThat(filter.get("path1"), equalTo(source.get("path1")));
        assertThat(filter.containsKey("test1"), equalTo(false));
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void testExtractValue() throws Exception {
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

        // fields with . in them
        builder = XContentFactory.jsonBuilder().startObject()
                .field("xxx.yyy", "value")
                .endObject();
        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractValue("xxx.yyy", map).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1.xxx").startObject("path2.yyy").field("test", "value").endObject().endObject()
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractValue("path1.xxx.path2.yyy.test", map).toString(), equalTo("value"));
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void testExtractRawValue() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("test", "value")
                .endObject();

        Map<String, Object> map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractRawValues("test", map).get(0).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .field("test.me", "value")
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractRawValues("test.me", map).get(0).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1").startObject("path2").field("test", "value").endObject().endObject()
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractRawValues("path1.path2.test", map).get(0).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1.xxx").startObject("path2.yyy").field("test", "value").endObject().endObject()
                .endObject();

        map = XContentFactory.xContent(XContentType.JSON).createParser(builder.string()).mapAndClose();
        assertThat(XContentMapValues.extractRawValues("path1.xxx.path2.yyy.test", map).get(0).toString(), equalTo("value"));
    }
}
