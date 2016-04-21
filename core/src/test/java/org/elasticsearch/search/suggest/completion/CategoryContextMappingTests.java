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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class CategoryContextMappingTests extends ESSingleNodeTestCase {

    public void testIndexingWithNoContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .array("input", "suggestion1", "suggestion2")
                .field("weight", 3)
                .endObject()
                .startObject()
                .array("input", "suggestion3", "suggestion4")
                .field("weight", 4)
                .endObject()
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 7);
    }

    public void testIndexingWithSimpleContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .startObject("contexts")
                .field("ctx", "ctx1")
                .endObject()
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithContextList() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", jsonBuilder()
                .startObject()
                .startObject("completion")
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .startObject("contexts")
                .array("ctx", "ctx1", "ctx2", "ctx3")
                .endObject()
                .field("weight", 5)
                .endObject()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMultipleContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .startObject()
                .field("name", "type")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .startObject("contexts")
                .array("ctx", "ctx1", "ctx2", "ctx3")
                .array("type", "typr3", "ftg")
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", builder.bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value("context1");
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
    }

    public void testQueryContextParsingArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                .value("context1")
                .value("context2")
                .endArray();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(2));
        assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
        assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
        assertThat(internalQueryContexts.get(1).boost, equalTo(1));
        assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
    }

    public void testQueryContextParsingObject() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("context", "context1")
                .field("boost", 10)
                .field("prefix", true)
                .endObject();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(10));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
    }


    public void testQueryContextParsingObjectArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
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
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(2));
        assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(2));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
        assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
        assertThat(internalQueryContexts.get(1).boost, equalTo(3));
        assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
    }

    private static QueryParseContext createParseContext(XContentParser parser) {
        return new QueryParseContext(new IndicesQueriesRegistry(), parser, ParseFieldMatcher.STRICT);
    }

    public void testQueryContextParsingMixed() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                .startObject()
                .field("context", "context1")
                .field("boost", 2)
                .field("prefix", true)
                .endObject()
                .value("context2")
                .endArray();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        CategoryContextMapping mapping = ContextBuilder.category("cat").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(2));
        assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(2));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
        assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
        assertThat(internalQueryContexts.get(1).boost, equalTo(1));
        assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
    }

    public void testParsingContextFromDocument() throws Exception {
        CategoryContextMapping mapping = ContextBuilder.category("cat").field("category").build();
        ParseContext.Document document = new ParseContext.Document();
        document.add(new StringField("category", "category1", Field.Store.NO));
        Set<CharSequence> context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(1));
        assertTrue(context.contains("category1"));
    }

    static void assertContextSuggestFields(IndexableField[] fields, int expected) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (field instanceof ContextSuggestField) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(expected));
    }
}
