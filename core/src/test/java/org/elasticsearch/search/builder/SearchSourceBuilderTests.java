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
package org.elasticsearch.search.builder;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class SearchSourceBuilderTests extends ESTestCase {

    SearchSourceBuilder builder = new SearchSourceBuilder();

    @Test // issue #6632
    public void testThatSearchSourceBuilderIncludesExcludesAreAppliedCorrectly() throws Exception {
        builder.fetchSource("foo", null);
        assertIncludes(builder, "foo");
        assertExcludes(builder);

        builder.fetchSource(null, "foo");
        assertIncludes(builder);
        assertExcludes(builder, "foo");

        builder.fetchSource(null, new String[]{"foo"});
        assertIncludes(builder);
        assertExcludes(builder, "foo");

        builder.fetchSource(new String[]{"foo"}, null);
        assertIncludes(builder, "foo");
        assertExcludes(builder);

        builder.fetchSource("foo", "bar");
        assertIncludes(builder, "foo");
        assertExcludes(builder, "bar");

        builder.fetchSource(new String[]{"foo"}, new String[]{"bar", "baz"});
        assertIncludes(builder, "foo");
        assertExcludes(builder, "bar", "baz");
    }

    private void assertIncludes(SearchSourceBuilder builder, String... elems) throws IOException {
        assertFieldValues(builder, "includes", elems);
    }

    private void assertExcludes(SearchSourceBuilder builder, String... elems) throws IOException {
        assertFieldValues(builder, "excludes", elems);
    }

    private void assertFieldValues(SearchSourceBuilder builder, String fieldName, String... elems) throws IOException {
        Map<String, Object> map = getSourceMap(builder);

        assertThat(map, hasKey(fieldName));
        assertThat(map.get(fieldName), is(instanceOf(List.class)));
        List<String> castedList = (List<String>) map.get(fieldName);
        assertThat(castedList, hasSize(elems.length));
        assertThat(castedList, hasItems(elems));
    }

    private Map<String, Object> getSourceMap(SearchSourceBuilder builder) throws IOException {
        Map<String, Object> data;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(builder.toString())) {
            data = parser.map();
        }
        assertThat(data, hasKey("_source"));
        return (Map<String, Object>) data.get("_source");
    }

}
