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

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;

public class CategoryContextMappingTest extends ElasticsearchTestCase {

    @Test
    public void testParsingQueryContextBasic() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().value("context1");
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        ContextMapping.QueryContexts<CategoryQueryContext> queryContexts = mapping.parseQueryContext("cat", parser);
        Iterator<CategoryQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        CategoryQueryContext queryContext = iterator.next();
        assertThat(queryContext.context.toString(), equalTo("context1"));
        assertThat(queryContext.boost, equalTo(1));
        assertThat(queryContext.isPrefix, equalTo(false));

    }

    @Test
    public void testParsingQueryContextArray() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startArray()
                .value("context1")
                .value("context2")
                .endArray();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        ContextMapping.QueryContexts<CategoryQueryContext> queryContexts = mapping.parseQueryContext("cat", parser);
        List<String> expectedContexts = new ArrayList<>(Arrays.asList("context1", "context2"));
        Iterator<CategoryQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        CategoryQueryContext queryContext = iterator.next();
        assertThat(queryContext.context.toString(), isIn(expectedContexts));
        assertTrue(iterator.hasNext());
        queryContext = iterator.next();
        assertThat(queryContext.context.toString(), isIn(expectedContexts));
    }

    @Test
    public void testParsingQueryContextObject() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("context", "context1")
                .field("boost", 10)
                .field("prefix", true)
                .endObject();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        ContextMapping.QueryContexts<CategoryQueryContext> queryContexts = mapping.parseQueryContext("cat", parser);
        Iterator<CategoryQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        CategoryQueryContext queryContext = iterator.next();
        assertThat(queryContext.context.toString(), equalTo("context1"));
        assertThat(queryContext.boost, equalTo(10));
        assertThat(queryContext.isPrefix, equalTo(true));
    }

    @Test
    public void testParsingQueryContextObjectArray() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startArray()
                .startObject()
                .field("context", "context1")
                .field("boost", 2)
                .field("prefix", true)
                .endObject()
                .startObject()
                .field("context", "context2")
                .field("boost", 3)
                .field("prefix", false)
                .endObject()
                .endArray();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        ContextMapping.QueryContexts<CategoryQueryContext> queryContexts = mapping.parseQueryContext("cat", parser);
        Iterator<CategoryQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        CategoryQueryContext queryContext = iterator.next();
        assertThat(queryContext.context.toString(), equalTo("context1"));
        assertThat(queryContext.boost, equalTo(2));
        assertThat(queryContext.isPrefix, equalTo(true));
        assertTrue(iterator.hasNext());
        queryContext = iterator.next();
        assertThat(queryContext.context.toString(), equalTo("context2"));
        assertThat(queryContext.boost, equalTo(3));
        assertThat(queryContext.isPrefix, equalTo(false));
    }

    @Test
    public void testParsingContextFromDocument() throws Exception {
        CategoryContextMapping mapping = ContextBuilder.category("cat").field("category").build();
        ParseContext.Document document = new ParseContext.Document();
        document.add(new StringField("category", "category1", Field.Store.NO));
        Set<CharSequence> context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(1));
        assertTrue(context.contains("category1"));
    }
}
