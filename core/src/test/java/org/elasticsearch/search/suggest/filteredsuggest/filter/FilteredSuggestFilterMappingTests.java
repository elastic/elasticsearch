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

package org.elasticsearch.search.suggest.filteredsuggest.filter;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FilteredSuggestFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.InternalQueryContext;
import org.elasticsearch.search.suggest.filteredsuggest.FilteredSuggestSuggestionBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class FilteredSuggestFilterMappingTests extends ESSingleNodeTestCase {

    public void testIndexingWithNoFilters() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "").startObject("filters").startObject("ctx")
                .field("type", "category").endObject().endObject().endObject().endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().startArray("suggest").startObject().array("input", "suggestion1", "suggestion2")
                        .field("weight", 3).endObject().startObject().array("input", "suggestion3", "suggestion4").field("weight", 4)
                        .endObject().startObject().array("input", "suggestion5", "suggestion6", "suggestion7").field("weight", 5)
                        .endObject().endArray().endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 7);
    }

    public void testNestedIndexingWithNoFilters() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ctx").field("type", "category").endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().field("name", "Adidas").startArray("Products").startObject().field("ProdName", "Runner")
                        .field("ProdType", "Shoes").startObject("suggest").array("input", "Adidas Showroom").endObject().endObject()
                        .endArray().endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getChildren("Products").get(0).getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 1);
    }

    public void testIndexingWithSimpleFilters() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "").startObject("filters").startObject("ctx")
                .field("type", "category").endObject().endObject().endObject().endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().startArray("suggest").startObject().array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("filters").field("ctx", "ctx1").endObject().field("weight", 5).endObject().endArray().endObject()
                        .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 3);
    }

    public void testNestedIndexingWithSimpleFilters() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ctx").field("type", "category").endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().field("name", "Adidas").startArray("Products").startObject().field("ProdName", "Runner")
                        .field("ProdType", "Shoes").startObject("suggest").array("input", "Adidas Showroom").startObject("filters")
                        .field("ctx", "ctx1").endObject().endObject().endObject().endArray().endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getChildren("Products").get(0).getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 1);
    }

    public void testIndexingWithFilterList() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "").startObject("filters").startObject("ctx")
                .field("type", "category").endObject().endObject().endObject().endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().startArray("suggest").startObject().array("input", "suggestion5", "suggestion6", "suggestion7")
                        .startObject("filters").array("ctx", "ctx1", "ctx2", "ctx3").endObject().field("weight", 5).endObject().endArray()
                        .endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 3);
    }

    public void testIndexingWithMultipleFilters() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "").startObject("filters").startObject("ctx")
                .field("type", "category").endObject().startObject("type").field("type", "category").endObject().endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        XContentBuilder builder = jsonBuilder().startObject().startArray("suggest").startObject()
                .array("input", "suggestion5", "suggestion6", "suggestion7").field("weight", 5).startObject("filters")
                .array("ctx", "ctx1", "ctx2", "ctx3").array("type", "typr3", "ftg").endObject().endObject().endArray().endObject();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", builder.bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 3);
    }

    public void testNestedIndexingWithSimpleFilterAsFieldReference() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().endObject().endObject()
                .endObject().endObject().endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().field("name", "Adidas").startArray("Products").startObject().field("ProdName", "Runner")
                        .field("ProdType", "Shoes").startObject("suggest").array("input", "Adidas Showroom").endObject().endObject()
                        .endArray().endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getChildren("Products").get(0).getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 1);
    }

    public void testNestedIndexingWithInputAndSimpleFilterAsFieldReference() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products")
                .field("input_fields", "Products.ProdName").startObject("filters").startObject("ProdType").field("type", "category")
                .field("path", "Products.ProdType").endObject().endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().field("name", "Adidas").startArray("Products").startObject().field("ProdName", "Runner")
                        .field("ProdType", "Shoes").startObject("suggest").endObject().endObject().endArray().endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getChildren("Products").get(0).getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 1);
    }

    public void testNestedIndexingWithInputAndSimpleFilterAsFieldReferenceAndDocHasNoSuggestField() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products")
                .field("input_fields", "Products.ProdName").startObject("filters").startObject("ProdType").field("type", "category")
                .field("path", "Products.ProdType").endObject().endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1",
                jsonBuilder().startObject().field("name", "Adidas").startArray("Products").startObject().field("ProdName", "Runner")
                        .field("ProdType", "Shoes").endObject().endArray().endObject().bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getChildren("Products").get(0).getFields(completionFieldType.name());
        assertFilterSuggestFields(fields, 1);
    }

    public void testQueryFilterParsingBasic() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ctx").field("type", "category").endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) completionFieldType);

        final long nowInMillis = randomNonNegativeLong();
        QueryShardContext context = new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, null,
                xContentRegistry(), null, null, () -> nowInMillis);

        Map<String, List<InternalQueryContext>> queries = FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(),
                null);

        List<ContextMapping.InternalQueryContext> internalQueryContexts = queries.get("default_empty");
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo(FilteredSuggestSuggestionBuilder.EMPTY_FILTER_FILLER));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
    }

    public void testQueryFilterParsingSingleFilterAsObject() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().endObject().endObject()
                .endObject().endObject().endObject().endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) completionFieldType);

        XContentBuilder builder = jsonBuilder().startObject().field("ProdType", "shoes").endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        parser.nextToken();

        final long nowInMillis = randomNonNegativeLong();
        QueryShardContext context = new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, null,
                xContentRegistry(), null, null, () -> nowInMillis);

        Map<String, List<InternalQueryContext>> queries = FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(),
                parser);

        List<ContextMapping.InternalQueryContext> internalQueryContexts = queries.get("ProdType");
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("0shoes"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
    }

    public void testQueryFilterParsingMultiFilterAsObject() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().startObject("Name")
                .field("type", "category").field("path", "name").endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) completionFieldType);

        XContentBuilder builder = jsonBuilder().startObject().field("ProdType", "shoes").field("Name", "adidas").endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        parser.nextToken();

        final long nowInMillis = randomNonNegativeLong();
        QueryShardContext context = new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, null,
                xContentRegistry(), null, null, () -> nowInMillis);

        Map<String, List<InternalQueryContext>> queries = FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(),
                parser);

        List<ContextMapping.InternalQueryContext> internalQueryContexts = queries.get("ProdType");
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("0shoes"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));

        internalQueryContexts = queries.get("Name");
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("1adidas"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
    }

    public void testQueryFilterParsingMultiFilterNestedAsObject() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("Designs").field("type", "nested").startObject("properties").startObject("DesignName").field("type", "text")
                .endObject().startObject("DesignCode").field("type", "text").endObject().endObject().endObject().startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "Products").field("input_fields", "name").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().startObject("DesignCode")
                .field("type", "category").field("path", "Products.Designs.DesignCode").endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) completionFieldType);

        XContentBuilder builder = jsonBuilder().startObject().field("ProdType", "shoes").field("DesignCode", "run001").endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        parser.nextToken();

        final long nowInMillis = randomNonNegativeLong();
        QueryShardContext context = new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, null,
                xContentRegistry(), null, null, () -> nowInMillis);

        Map<String, List<InternalQueryContext>> queries = FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(),
                parser);
        List<ContextMapping.InternalQueryContext> internalQueryContexts = queries.get("ProdTypeDesignCode");

        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("0shoes1run001"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
    }

    public void testQueryFilterParsingMultiFilterNestedWithArrayValues() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("Designs").field("type", "nested").startObject("properties").startObject("DesignName").field("type", "text")
                .endObject().startObject("DesignCode").field("type", "text").endObject().endObject().endObject().startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "Products").field("input_fields", "name").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().startObject("DesignCode")
                .field("type", "category").field("path", "Products.Designs.DesignCode").endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) completionFieldType);

        XContentBuilder builder = jsonBuilder().startObject().array("ProdType", "shoes", "laces").field("DesignCode", "run001").endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        parser.nextToken();

        final long nowInMillis = randomNonNegativeLong();
        QueryShardContext context = new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, null,
                xContentRegistry(), null, null, () -> nowInMillis);

        Map<String, List<InternalQueryContext>> queries = FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(),
                parser);
        List<ContextMapping.InternalQueryContext> internalQueryContexts = queries.get("ProdTypeDesignCode");

        assertThat(internalQueryContexts.size(), equalTo(2));
        assertThat(internalQueryContexts.get(0).context, equalTo("0shoes1run001"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(1));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));

        assertThat(internalQueryContexts.get(1).context, equalTo("0laces1run001"));
        assertThat(internalQueryContexts.get(1).boost, equalTo(1));
        assertThat(internalQueryContexts.get(1).isPrefix, equalTo(false));
    }

    public void testQueryFilterParsingSingleFilterObjectWithBoost() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("suggest").field("type", "filteredsuggest").field("parent_path", "Products").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().endObject().endObject()
                .endObject().endObject().endObject().endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("Products.suggest");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) completionFieldType);

        XContentBuilder builder = jsonBuilder().startObject().startObject("ProdType").field("value", "shoes").field("boost", 5).endObject()
                .endObject();

        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        parser.nextToken();

        final long nowInMillis = randomNonNegativeLong();
        QueryShardContext context = new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, null,
                xContentRegistry(), null, null, () -> nowInMillis);

        Map<String, List<InternalQueryContext>> queries = FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(),
                parser);

        List<ContextMapping.InternalQueryContext> internalQueryContexts = queries.get("ProdType");
        assertThat(internalQueryContexts.size(), equalTo(1));
        assertThat(internalQueryContexts.get(0).context, equalTo("0shoes"));
        assertThat(internalQueryContexts.get(0).boost, equalTo(5));
        assertThat(internalQueryContexts.get(0).isPrefix, equalTo(false));
    }

    public void testDocumentIndexMultiFilterNestedAsObjectHavingInputAtDifferentDocLevel() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("name")
                .field("type", "text").endObject().startObject("Products").field("type", "nested").startObject("properties")
                .startObject("ProdName").field("type", "text").endObject().startObject("ProdType").field("type", "text").endObject()
                .startObject("Designs").field("type", "nested").startObject("properties").startObject("DesignName").field("type", "text")
                .endObject().startObject("DesignCode").field("type", "text").endObject().endObject().endObject().startObject("suggest")
                .field("type", "filteredsuggest").field("parent_path", "Products").field("input_fields", "name").startObject("filters")
                .startObject("ProdType").field("type", "category").field("path", "Products.ProdType").endObject().startObject("DesignCode")
                .field("type", "category").field("path", "Products.Designs.DesignCode").endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1",
                new CompressedXContent(mapping));
        try {
            defaultMapper.parse("test", "type1", "1",
                    jsonBuilder().startObject().field("name", "Adi Showroom").startArray("Products").startObject()
                            .field("ProdName", "Runner").field("ProdType", "Shoes").startArray("Designs").startObject()
                            .field("DesignName", "Runner").field("DesignCode", "Shoes").endObject().endArray().endObject().endArray()
                            .endObject().bytes());
            fail("input field reference at different doc level is illegal");
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(),
                    "Input field [name] must be at the same document hierarchy level where suggestion field is defined");
        }
    }

    static void assertFilterSuggestFields(IndexableField[] fields, int expected) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (FilterMappings.isFilteredSuggestField(field)) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(expected));
    }

}
