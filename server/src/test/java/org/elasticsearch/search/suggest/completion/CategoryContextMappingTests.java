/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.CompletionFieldMapper.CompletionFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CategoryContextMappingTests extends MapperServiceTestCase {

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "simple",
                new NamedAnalyzer("simple", AnalyzerScope.INDEX, new SimpleAnalyzer())
            )
        );
    }

    public void testIndexingWithNoContexts() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    jsonBuilder().startObject()
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
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 7);
    }

    public void testIndexingWithSimpleContexts() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    jsonBuilder().startObject()
                        .startArray("completion")
                        .startObject()
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .field("ctx", "ctx1")
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithSimpleNumberContexts() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    jsonBuilder().startObject()
                        .startArray("completion")
                        .startObject()
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .field("ctx", 100)
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithSimpleBooleanContexts() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    jsonBuilder().startObject()
                        .startArray("completion")
                        .startObject()
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .field("ctx", true)
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithSimpleNULLContexts() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        XContentBuilder builder = jsonBuilder().startObject()
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

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("1", BytesReference.bytes(builder), XContentType.JSON))
        );
        assertEquals(
            "contexts must be a string, number or boolean or a list of string, number or boolean, but was [VALUE_NULL]",
            e.getCause().getMessage()
        );
    }

    public void testIndexingWithContextList() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    jsonBuilder().startObject()
                        .startObject("completion")
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .array("ctx", "ctx1", "ctx2", "ctx3")
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMixedTypeContextList() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    jsonBuilder().startObject()
                        .startObject("completion")
                        .array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("contexts")
                        .array("ctx", "ctx1", true, 100)
                        .endObject()
                        .field("weight", 5)
                        .endObject()
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMixedTypeContextListHavingNULL() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("completion")
            .array("input", "suggestion5", "suggestion6", "suggestion7")
            .startObject("contexts")
            .array("ctx", "ctx1", true, 100, null)
            .endObject()
            .field("weight", 5)
            .endObject()
            .endObject();

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("1", BytesReference.bytes(builder), XContentType.JSON))
        );
        assertEquals("context array must have string, number or boolean values, but was [VALUE_NULL]", e.getCause().getMessage());
    }

    public void testIndexingWithMultipleContexts() throws Exception {
        String mapping = Strings.toString(
            jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("completion")
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
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        XContentBuilder builder = jsonBuilder().startObject()
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
        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("1", BytesReference.bytes(builder), XContentType.JSON));
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value("context1");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(false));
        }
    }

    public void testBooleanQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value(true);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context(), equalTo("true"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(false));
        }
    }

    public void testNumberQueryContextParsingBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value(10);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context(), equalTo("10"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(false));
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
        XContentBuilder builder = jsonBuilder().startArray().value("context1").value("context2").endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(2));
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(1).context(), equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(1).isPrefix(), equalTo(false));
        }
    }

    public void testQueryContextParsingMixedTypeValuesArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray().value("context1").value("context2").value(true).value(10).endArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(4));
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(1).context(), equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(1).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(2).context(), equalTo("true"));
            assertThat(internalQueryContexts.get(2).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(2).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(3).context(), equalTo("10"));
            assertThat(internalQueryContexts.get(3).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(3).isPrefix(), equalTo(false));
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
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(10));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(true));
        }
    }

    public void testQueryContextParsingObjectHavingBoolean() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("context", false).field("boost", 10).field("prefix", true).endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context(), equalTo("false"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(10));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(true));
        }
    }

    public void testQueryContextParsingObjectHavingNumber() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("context", 333).field("boost", 10).field("prefix", true).endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            CategoryContextMapping mapping = ContextBuilder.category("cat").build();
            List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
            assertThat(internalQueryContexts.size(), equalTo(1));
            assertThat(internalQueryContexts.get(0).context(), equalTo("333"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(10));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(true));
        }
    }

    public void testQueryContextParsingObjectHavingNULL() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().nullField("context").field("boost", 10).field("prefix", true).endObject();
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
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(2));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(true));
            assertThat(internalQueryContexts.get(1).context(), equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost(), equalTo(3));
            assertThat(internalQueryContexts.get(1).isPrefix(), equalTo(false));
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
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(2));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(true));
            assertThat(internalQueryContexts.get(1).context(), equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost(), equalTo(3));
            assertThat(internalQueryContexts.get(1).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(2).context(), equalTo("true"));
            assertThat(internalQueryContexts.get(2).boost(), equalTo(3));
            assertThat(internalQueryContexts.get(2).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(3).context(), equalTo("333"));
            assertThat(internalQueryContexts.get(3).boost(), equalTo(3));
            assertThat(internalQueryContexts.get(3).isPrefix(), equalTo(false));
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
            assertThat(internalQueryContexts.get(0).context(), equalTo("context1"));
            assertThat(internalQueryContexts.get(0).boost(), equalTo(2));
            assertThat(internalQueryContexts.get(0).isPrefix(), equalTo(true));
            assertThat(internalQueryContexts.get(1).context(), equalTo("context2"));
            assertThat(internalQueryContexts.get(1).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(1).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(2).context(), equalTo("false"));
            assertThat(internalQueryContexts.get(2).boost(), equalTo(1));
            assertThat(internalQueryContexts.get(2).isPrefix(), equalTo(false));
            assertThat(internalQueryContexts.get(3).context(), equalTo("333"));
            assertThat(internalQueryContexts.get(3).boost(), equalTo(2));
            assertThat(internalQueryContexts.get(3).isPrefix(), equalTo(true));
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
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("completion")
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
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);
        CompletionFieldType completionFieldType = (CompletionFieldType) mapperService.fieldType("completion");

        Exception e = expectThrows(IllegalArgumentException.class, () -> completionFieldType.getContextMappings().get("brand"));
        assertEquals("Unknown context name [brand], must be one of [ctx, type]", e.getMessage());
    }

    public void testParsingContextFromDocument() {
        CategoryContextMapping mapping = ContextBuilder.category("cat").field("category").build();
        LuceneDocument document = new LuceneDocument();

        KeywordFieldMapper.KeywordFieldType keyword = new KeywordFieldMapper.KeywordFieldType("category");
        document.add(new KeywordFieldMapper.KeywordField(keyword.name(), new BytesRef("category1"), new FieldType()));
        // Ignore doc values
        document.add(new SortedSetDocValuesField(keyword.name(), new BytesRef("category1")));
        Set<String> context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(1));
        assertTrue(context.contains("category1"));

        document = new LuceneDocument();
        TextFieldMapper.TextFieldType text = new TextFieldMapper.TextFieldType("category", randomBoolean());
        document.add(new Field(text.name(), "category1", TextFieldMapper.Defaults.FIELD_TYPE));
        // Ignore stored field
        document.add(new StoredField(text.name(), "category1", TextFieldMapper.Defaults.FIELD_TYPE));
        context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(1));
        assertTrue(context.contains("category1"));

        document = new LuceneDocument();
        document.add(new SortedSetDocValuesField("category", new BytesRef("category")));
        context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(0));

        document = new LuceneDocument();
        document.add(new SortedDocValuesField("category", new BytesRef("category")));
        context = mapping.parseContext(document);
        assertThat(context.size(), equalTo(0));

        final LuceneDocument doc = new LuceneDocument();
        doc.add(new IntPoint("category", 36));
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> mapping.parseContext(doc));
        assertThat(exc.getMessage(), containsString("Failed to parse context field [category]"));
    }

    static void assertContextSuggestFields(List<IndexableField> fields, int expected) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (field instanceof ContextSuggestField) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(expected));
    }
}
