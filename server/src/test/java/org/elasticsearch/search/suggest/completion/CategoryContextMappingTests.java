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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.CompletionFieldMapper.CompletionFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CategoryContextMappingTests extends ESSingleNodeTestCase {

    public void testIndexingWithNoContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
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
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 7);
    }

    public void testIndexingWithSimpleContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .startArray("completion")
                        .startObject()
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .field("ctx", "ctx1")
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithSimpleNumberContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .startArray("completion")
                        .startObject()
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .field("ctx", 100)
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()),
            XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithSimpleBooleanContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .startArray("completion")
                        .startObject()
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .field("ctx", true)
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()),
            XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithSimpleNULLContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .array("input", "suggestion5", "suggestion6", "suggestion7")
                .startObject("contexts")
                .nullField("ctx")
                .endObject()
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject();

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1", BytesReference.bytes(builder), XContentType.JSON)));
        assertEquals("contexts must be a string, number or boolean or a list of string, number or boolean, but was [VALUE_NULL]",
                e.getCause().getMessage());
    }

    public void testIndexingWithContextList() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .startObject("completion")
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .array("ctx", "ctx1", "ctx2", "ctx3")
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMixedTypeContextList() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .startObject("completion")
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .array("ctx", "ctx1", true, 100)
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endObject()),
            XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMixedTypeContextListHavingNULL() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("completion")
                .array("input", "suggestion5", "suggestion6", "suggestion7")
                .startObject("contexts")
                .array("ctx", "ctx1", true, 100, null)
                .endObject()
                .field("weight", 5)
                .endObject()
                .endObject();

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1", BytesReference.bytes(builder), XContentType.JSON)));
        assertEquals("context array must have string, number or boolean values, but was [VALUE_NULL]", e.getCause().getMessage());
    }

    public void testIndexingWithMultipleContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
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
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .array("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .startObject("contexts")
                .array("ctx", "ctx1", "ctx2", "ctx3")
                .array("type", "typr3", "ftg")
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1", BytesReference.bytes(builder),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value("context1");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
        }
    }

    public void testBooleanQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value(true);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context, equalTo("true"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
        }
    }

    public void testNumberQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value(10);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context, equalTo("10"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
        }
    }

    public void testNULLQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().nullValue();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();

            XContentParseException e = expectThrows(XContentParseException.class, () -> mapping.parseQueryContext(parser));
            assertThat(e.getMessage(), containsString("category context must be an object, string, number or boolean"));
        }
    }

    public void testQueryContextParsingArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                    .value("context1")
                    .value("context2")
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(2));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost, equalTo(1));
            assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
        }
    }

    public void testQueryContextParsingMixedTypeValuesArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                    .value("context1")
                    .value("context2")
                    .value(true)
                    .value(10)
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(4));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost, equalTo(1));
            assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(2).context, equalTo("true"));
            assertThat(internalQueryContexts.get(2).boost, equalTo(1));
            assertThat(internalQueryContexts.get(2).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(3).context, equalTo("10"));
            assertThat(internalQueryContexts.get(3).boost, equalTo(1));
            assertThat(internalQueryContexts.get(3).isPrefix, equalTo(false));
        }
    }

    public void testQueryContextParsingMixedTypeValuesArrayHavingNULL() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                    .value("context1")
                    .value("context2")
                    .value(true)
                    .value(10)
                    .nullValue()
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();

            XContentParseException e = expectThrows(XContentParseException.class, () -> mapping.parseQueryContext(parser));
            assertThat(e.getMessage(), containsString("category context must be an object, string, number or boolean"));
        }
    }

    public void testQueryContextParsingObject() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("context", "context1")
                .field("boost", 10)
                .field("prefix", true)
                .endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(10));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
        }
    }

    public void testQueryContextParsingObjectHavingBoolean() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("context", false)
                .field("boost", 10)
                .field("prefix", true)
                .endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context, equalTo("false"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(10));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
        }
    }

    public void testQueryContextParsingObjectHavingNumber() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("context", 333)
                .field("boost", 10)
                .field("prefix", true)
                .endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context, equalTo("333"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(10));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
        }
    }

    public void testQueryContextParsingObjectHavingNULL() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .nullField("context")
                .field("boost", 10)
                .field("prefix", true)
                .endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();

            Exception e = expectThrows(XContentParseException.class, () -> mapping.parseQueryContext(parser));
            assertThat(e.getMessage(), containsString("category context must be a string, number or boolean"));
        }
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
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(2));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(2));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
            assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost, equalTo(3));
            assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
        }
    }

    public void testQueryContextParsingMixedTypeObjectArray() throws Exception {
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
                .startObject()
                .field("context", true)
                .field("boost", 3)
                .field("prefix", false)
                .endObject()
                .startObject()
                .field("context", 333)
                .field("boost", 3)
                .field("prefix", false)
                .endObject()
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(4));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(2));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
            assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost, equalTo(3));
            assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(2).context, equalTo("true"));
            assertThat(internalQueryContexts.get(2).boost, equalTo(3));
            assertThat(internalQueryContexts.get(2).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(3).context, equalTo("333"));
            assertThat(internalQueryContexts.get(3).boost, equalTo(3));
            assertThat(internalQueryContexts.get(3).isPrefix, equalTo(false));
        }
    }

    public void testQueryContextParsingMixedTypeObjectArrayHavingNULL() throws Exception {
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
                .startObject()
                .field("context", true)
                .field("boost", 3)
                .field("prefix", false)
                .endObject()
                .startObject()
                .field("context", 333)
                .field("boost", 3)
                .field("prefix", false)
                .endObject()
                .startObject()
                .nullField("context")
                .field("boost", 3)
                .field("prefix", false)
                .endObject()
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();

            XContentParseException e = expectThrows(XContentParseException.class, () -> mapping.parseQueryContext(parser));
            assertThat(e.getMessage(), containsString("category context must be a string, number or boolean"));
        }
    }


    public void testQueryContextParsingMixed() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                .startObject()
                .field("context", "context1")
                .field("boost", 2)
                .field("prefix", true)
                .endObject()
                .value("context2")
                .value(false)
                .startObject()
                .field("context", 333)
                .field("boost", 2)
                .field("prefix", true)
                .endObject()
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(4));
            assertThat(internalQueryContexts.get(0).context, equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost, equalTo(2));
            assertThat(internalQueryContexts.get(0).isPrefix, equalTo(true));
            assertThat(internalQueryContexts.get(1).context, equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost, equalTo(1));
            assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(2).context, equalTo("false"));
            assertThat(internalQueryContexts.get(2).boost, equalTo(1));
            assertThat(internalQueryContexts.get(2).isPrefix, equalTo(false));
            assertThat(internalQueryContexts.get(3).context, equalTo("333"));
            assertThat(internalQueryContexts.get(3).boost, equalTo(2));
            assertThat(internalQueryContexts.get(3).isPrefix, equalTo(true));
        }
    }

    public void testQueryContextParsingMixedHavingNULL() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
                .startObject()
                .field("context", "context1")
                .field("boost", 2)
                .field("prefix", true)
                .endObject()
                .value("context2")
                .value(false)
                .startObject()
                .field("context", 333)
                .field("boost", 2)
                .field("prefix", true)
                .endObject()
                .nullValue()
                .endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();

            XContentParseException e = expectThrows(XContentParseException.class, () -> mapping.parseQueryContext(parser));
            assertThat(e.getMessage(), containsString("category context must be an object, string, number or boolean"));
        }
    }

    public void testUnknownQueryContextParsing() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc")
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
                .endObject().endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        CompletionFieldType completionFieldType = (CompletionFieldType) mapperService.fieldType("completion");

        Exception e = expectThrows(IllegalArgumentException.class, () -> completionFieldType.getContextMappings().get("brand"));
        assertEquals("Unknown context name [brand], must be one of [ctx, type]", e.getMessage());
    }

    public void testParsingContextFromDocument() throws Exception {
        CategoryContextMapping mapping = ContextBuilder.category("cat").field("category").build();
        ParseContext.Document document = new ParseContext.Document();

        KeywordFieldMapper.KeywordFieldType keyword = new KeywordFieldMapper.KeywordFieldType();
        keyword.setName("category");
        document.add(new Field(keyword.name(), new BytesRef("category1"), keyword));
        // Ignore doc values
        document.add(new SortedSetDocValuesField(keyword.name(), new BytesRef("category1")));
        Set<String> context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(1));
        assertTrue(context.contains("category1"));


        document = new ParseContext.Document();
        TextFieldMapper.TextFieldType text = new TextFieldMapper.TextFieldType();
        text.setName("category");
        document.add(new Field(text.name(), "category1", text));
        // Ignore stored field
        document.add(new StoredField(text.name(), "category1", text));
        context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(1));
        assertTrue(context.contains("category1"));

        document = new ParseContext.Document();
        document.add(new SortedSetDocValuesField("category", new BytesRef("category")));
        context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(0));

        document = new ParseContext.Document();
        document.add(new SortedDocValuesField("category", new BytesRef("category")));
        context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(0));

        final ParseContext.Document doc = new ParseContext.Document();
        doc.add(new IntPoint("category", 36));
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> mapping.parseContext(doc));
        assertThat(exc.getMessage(), containsString("Failed to parse context field [category]"));
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
