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

package org.elasticsearch.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsLookupQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.percolator.PercolatorFieldMapper.EXTRACTION_COMPLETE;
import static org.elasticsearch.percolator.PercolatorFieldMapper.EXTRACTION_FAILED;
import static org.elasticsearch.percolator.PercolatorFieldMapper.EXTRACTION_PARTIAL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PercolatorFieldMapperTests extends ESSingleNodeTestCase {

    private String fieldName;
    private IndexService indexService;
    private MapperService mapperService;
    private PercolatorFieldMapper.FieldType fieldType;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, PercolatorPlugin.class, FoolMeScriptPlugin.class, ParentJoinPlugin.class);
    }

    @Before
    public void init() throws Exception {
        indexService = createIndex("test");
        mapperService = indexService.mapperService();

        String mapper = XContentFactory.jsonBuilder().startObject().startObject("doc")
            .startObject("_field_names").field("enabled", false).endObject() // makes testing easier
            .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .startObject("field1").field("type", "text").endObject()
                .startObject("field2").field("type", "text").endObject()
                .startObject("_field3").field("type", "text").endObject()
                .startObject("field4").field("type", "text").endObject()
                .startObject("number_field").field("type", "long").endObject()
                .startObject("date_field").field("type", "date").endObject()
            .endObject().endObject().endObject().string();
        mapperService.merge("doc", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    private void addQueryFieldMappings() throws Exception {
        fieldName = randomAlphaOfLength(4);
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .startObject("properties").startObject(fieldName).field("type", "percolator").endObject().endObject()
                .endObject().endObject().string();
        mapperService.merge("doc", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, false);
        fieldType = (PercolatorFieldMapper.FieldType) mapperService.fullName(fieldName);
    }

    public void testExtractTerms() throws Exception {
        addQueryFieldMappings();
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("field", "term1"));
        bq.add(termQuery1, BooleanClause.Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("field", "term2"));
        bq.add(termQuery2, BooleanClause.Occur.SHOULD);

        DocumentMapper documentMapper = mapperService.documentMapper("doc");
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        ParseContext.InternalParseContext parseContext = new ParseContext.InternalParseContext(Settings.EMPTY,
                mapperService.documentMapperParser(), documentMapper, null, null);
        fieldMapper.processQuery(bq.build(), parseContext);
        ParseContext.Document document = parseContext.doc();

        PercolatorFieldMapper.FieldType fieldType = (PercolatorFieldMapper.FieldType) fieldMapper.fieldType();
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_COMPLETE));
        List<IndexableField> fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.queryTermsField.name())));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0).binaryValue().utf8ToString(), equalTo("field\u0000term1"));
        assertThat(fields.get(1).binaryValue().utf8ToString(), equalTo("field\u0000term2"));
    }

    public void testExtractTermsAndRanges_failed() throws Exception {
        addQueryFieldMappings();
        TermRangeQuery query = new TermRangeQuery("field1", new BytesRef("a"), new BytesRef("z"), true, true);
        DocumentMapper documentMapper = mapperService.documentMapper("doc");
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        ParseContext.InternalParseContext parseContext = new ParseContext.InternalParseContext(Settings.EMPTY,
                mapperService.documentMapperParser(), documentMapper, null, null);
        fieldMapper.processQuery(query, parseContext);
        ParseContext.Document document = parseContext.doc();

        PercolatorFieldMapper.FieldType fieldType = (PercolatorFieldMapper.FieldType) fieldMapper.fieldType();
        assertThat(document.getFields().size(), equalTo(1));
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_FAILED));
    }

    public void testExtractTermsAndRanges_partial() throws Exception {
        addQueryFieldMappings();
        PhraseQuery phraseQuery = new PhraseQuery("field", "term");
        DocumentMapper documentMapper = mapperService.documentMapper("doc");
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        ParseContext.InternalParseContext parseContext = new ParseContext.InternalParseContext(Settings.EMPTY,
                mapperService.documentMapperParser(), documentMapper, null, null);
        fieldMapper.processQuery(phraseQuery, parseContext);
        ParseContext.Document document = parseContext.doc();

        PercolatorFieldMapper.FieldType fieldType = (PercolatorFieldMapper.FieldType) fieldMapper.fieldType();
        assertThat(document.getFields().size(), equalTo(2));
        assertThat(document.getFields().get(0).binaryValue().utf8ToString(), equalTo("field\u0000term"));
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
    }

    public void testCreateCandidateQuery() throws Exception {
        addQueryFieldMappings();

        MemoryIndex memoryIndex = new MemoryIndex(false);
        memoryIndex.addField("field1", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        memoryIndex.addField("field2", "some more text", new WhitespaceAnalyzer());
        memoryIndex.addField("_field3", "unhide me", new WhitespaceAnalyzer());
        memoryIndex.addField("field4", "123", new WhitespaceAnalyzer());
        memoryIndex.addField(new LongPoint("number_field", 10L), new WhitespaceAnalyzer());

        IndexReader indexReader = memoryIndex.createSearcher().getIndexReader();

        BooleanQuery candidateQuery = (BooleanQuery) fieldType.createCandidateQuery(indexReader);
        assertEquals(2, candidateQuery.clauses().size());
        assertEquals(Occur.SHOULD, candidateQuery.clauses().get(0).getOccur());
        TermInSetQuery termsQuery = (TermInSetQuery) candidateQuery.clauses().get(0).getQuery();

        PrefixCodedTerms terms = termsQuery.getTermData();
        assertThat(terms.size(), equalTo(14L));
        PrefixCodedTerms.TermIterator termIterator = terms.iterator();
        assertTermIterator(termIterator, "_field3\u0000me", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "_field3\u0000unhide", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000brown", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000dog", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000fox", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000jumps", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000lazy", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000over", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000quick", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field1\u0000the", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field2\u0000more", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field2\u0000some", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field2\u0000text", fieldType.queryTermsField.name());
        assertTermIterator(termIterator, "field4\u0000123", fieldType.queryTermsField.name());

        assertEquals(Occur.SHOULD, candidateQuery.clauses().get(1).getOccur());
        assertEquals(new TermQuery(new Term(fieldType.extractionResultField.name(), EXTRACTION_FAILED)),
                candidateQuery.clauses().get(1).getQuery());
    }

    private void assertTermIterator(PrefixCodedTerms.TermIterator termIterator, String expectedValue, String expectedField) {
        assertThat(termIterator.next().utf8ToString(), equalTo(expectedValue));
        assertThat(termIterator.field(), equalTo(expectedField));
    }

    public void testPercolatorFieldMapper() throws Exception {
        addQueryFieldMappings();
        QueryBuilder queryBuilder = termQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1", XContentFactory
                .jsonBuilder()
                .startObject()
                .field(fieldName, queryBuilder)
                .endObject().bytes(),
                XContentType.JSON));

        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name())[0].binaryValue().utf8ToString(), equalTo("field\0value"));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name())[0].stringValue(),
                equalTo(EXTRACTION_COMPLETE));
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);

        // add an query for which we don't extract terms from
        queryBuilder = rangeQuery("field").from("a").to("z");
        doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1", XContentFactory
                .jsonBuilder()
                .startObject()
                .field(fieldName, queryBuilder)
                .endObject().bytes(),
                XContentType.JSON));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name())[0].stringValue(),
                equalTo(EXTRACTION_FAILED));
        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name()).length, equalTo(0));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()).length, equalTo(1));
        qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);
    }

    public void testStoringQueries() throws Exception {
        addQueryFieldMappings();
        QueryBuilder[] queries = new QueryBuilder[]{
                termQuery("field", "value"), matchAllQuery(), matchQuery("field", "value"), matchPhraseQuery("field", "value"),
                prefixQuery("field", "v"), wildcardQuery("field", "v*"), rangeQuery("number_field").gte(0).lte(9),
                rangeQuery("date_field").from("2015-01-01T00:00").to("2015-01-01T00:00")
        };
        // note: it important that range queries never rewrite, otherwise it will cause results to be wrong.
        // (it can't use shard data for rewriting purposes, because percolator queries run on MemoryIndex)

        for (QueryBuilder query : queries) {
            ParsedDocument doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1",
                    XContentFactory.jsonBuilder().startObject()
                    .field(fieldName, query)
                    .endObject().bytes(),
                    XContentType.JSON));
            BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
            assertQueryBuilder(qbSource, query);
        }
    }

    public void testQueryWithRewrite() throws Exception {
        addQueryFieldMappings();
        client().prepareIndex("remote", "doc", "1").setSource("field", "value").get();
        QueryBuilder queryBuilder = termsLookupQuery("field", new TermsLookup("remote", "doc", "1", "field"));
        ParsedDocument doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1", XContentFactory
                .jsonBuilder()
                .startObject()
                .field(fieldName, queryBuilder)
                .endObject().bytes(),
                XContentType.JSON));
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder.rewrite(indexService.newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); })));
    }


    public void testPercolatorFieldMapperUnMappedField() throws Exception {
        addQueryFieldMappings();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1", XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .field(fieldName, termQuery("unmapped_field", "value"))
                    .endObject().bytes(),
                    XContentType.JSON));
        });
        assertThat(exception.getCause(), instanceOf(QueryShardException.class));
        assertThat(exception.getCause().getMessage(), equalTo("No field mapping can be found for the field with name [unmapped_field]"));
    }


    public void testPercolatorFieldMapper_noQuery() throws Exception {
        addQueryFieldMappings();
        ParsedDocument doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1", XContentFactory
                .jsonBuilder()
                .startObject()
                .endObject()
                .bytes(),
                XContentType.JSON));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()).length, equalTo(0));

        try {
            mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1", XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .nullField(fieldName)
                    .endObject().bytes(),
                    XContentType.JSON));
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("query malformed, must start with start_object"));
        }
    }

    public void testAllowNoAdditionalSettings() throws Exception {
        addQueryFieldMappings();
        IndexService indexService = createIndex("test1", Settings.EMPTY);
        MapperService mapperService = indexService.mapperService();

        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject("doc")
            .startObject("properties").startObject(fieldName).field("type", "percolator").field("index", "no").endObject().endObject()
            .endObject().endObject().string();
        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapperService.merge("doc", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true));
        assertThat(e.getMessage(), containsString("Mapping definition for [" + fieldName + "] has unsupported parameters:  [index : no]"));
    }

    // multiple percolator fields are allowed in the mapping, but only one field can be used at index time.
    public void testMultiplePercolatorFields() throws Exception {
        String typeName = "doc";
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("_field_names").field("enabled", false).endObject() // makes testing easier
                .startObject("properties")
                    .startObject("query_field1").field("type", "percolator").endObject()
                    .startObject("query_field2").field("type", "percolator").endObject()
                .endObject()
                .endObject().endObject().string();
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper(typeName).parse(SourceToParse.source("test", typeName, "1",
                jsonBuilder().startObject()
                        .field("query_field1", queryBuilder)
                        .field("query_field2", queryBuilder)
                        .endObject().bytes(),
                        XContentType.JSON));
        assertThat(doc.rootDoc().getFields().size(), equalTo(12)); // also includes all other meta fields
        BytesRef queryBuilderAsBytes = doc.rootDoc().getField("query_field1.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        queryBuilderAsBytes = doc.rootDoc().getField("query_field2.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);
    }

    // percolator field can be nested under an object field, but only one query can be specified per document
    public void testNestedPercolatorField() throws Exception {
        String typeName = "doc";
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("_field_names").field("enabled", false).endObject() // makes testing easier
                .startObject("properties")
                .startObject("object_field")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("query_field").field("type", "percolator").endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper(typeName).parse(SourceToParse.source("test", typeName, "1",
                jsonBuilder().startObject().startObject("object_field")
                            .field("query_field", queryBuilder)
                        .endObject().endObject().bytes(),
                        XContentType.JSON));
        assertThat(doc.rootDoc().getFields().size(), equalTo(9)); // also includes all other meta fields
        BytesRef queryBuilderAsBytes = doc.rootDoc().getField("object_field.query_field.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        doc = mapperService.documentMapper(typeName).parse(SourceToParse.source("test", typeName, "1",
                jsonBuilder().startObject()
                            .startArray("object_field")
                                .startObject().field("query_field", queryBuilder).endObject()
                            .endArray()
                        .endObject().bytes(),
                        XContentType.JSON));
        assertThat(doc.rootDoc().getFields().size(), equalTo(9)); // also includes all other meta fields
        queryBuilderAsBytes = doc.rootDoc().getField("object_field.query_field.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
                    mapperService.documentMapper(typeName).parse(SourceToParse.source("test", typeName, "1",
                            jsonBuilder().startObject()
                                    .startArray("object_field")
                                        .startObject().field("query_field", queryBuilder).endObject()
                                        .startObject().field("query_field", queryBuilder).endObject()
                                    .endArray()
                                .endObject().bytes(),
                                XContentType.JSON));
                }
        );
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("a document can only contain one percolator query"));
    }

    public void testUnsupportedQueries() {
        RangeQueryBuilder rangeQuery1 = new RangeQueryBuilder("field").from("2016-01-01||/D").to("2017-01-01||/D");
        RangeQueryBuilder rangeQuery2 = new RangeQueryBuilder("field").from("2016-01-01||/D").to("now");
        PercolatorFieldMapper.verifyQuery(rangeQuery1);
        PercolatorFieldMapper.verifyQuery(rangeQuery2);

        HasChildQueryBuilder hasChildQuery = new HasChildQueryBuilder("_type", new MatchAllQueryBuilder(), ScoreMode.None);
        expectThrows(IllegalArgumentException.class, () ->
                PercolatorFieldMapper.verifyQuery(new BoolQueryBuilder().must(hasChildQuery)));
        expectThrows(IllegalArgumentException.class, () ->
                PercolatorFieldMapper.verifyQuery(new DisMaxQueryBuilder().add(hasChildQuery)));
        PercolatorFieldMapper.verifyQuery(new ConstantScoreQueryBuilder((rangeQuery1)));
        expectThrows(IllegalArgumentException.class, () ->
                PercolatorFieldMapper.verifyQuery(new ConstantScoreQueryBuilder(hasChildQuery)));
        PercolatorFieldMapper.verifyQuery(new BoostingQueryBuilder(rangeQuery1, new MatchAllQueryBuilder()));
        expectThrows(IllegalArgumentException.class, () ->
                PercolatorFieldMapper.verifyQuery(new BoostingQueryBuilder(hasChildQuery, new MatchAllQueryBuilder())));
        PercolatorFieldMapper.verifyQuery(new FunctionScoreQueryBuilder(rangeQuery1, new RandomScoreFunctionBuilder()));
        expectThrows(IllegalArgumentException.class, () ->
                PercolatorFieldMapper.verifyQuery(new FunctionScoreQueryBuilder(hasChildQuery, new RandomScoreFunctionBuilder())));

        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.verifyQuery(hasChildQuery));
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.verifyQuery(new BoolQueryBuilder().must(hasChildQuery)));

        HasParentQueryBuilder hasParentQuery = new HasParentQueryBuilder("_type", new MatchAllQueryBuilder(), false);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.verifyQuery(hasParentQuery));
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.verifyQuery(new BoolQueryBuilder().must(hasParentQuery)));
    }

    private void assertQueryBuilder(BytesRef actual, QueryBuilder expected) throws IOException {
        XContentParser sourceParser = createParser(PercolatorFieldMapper.QUERY_BUILDER_CONTENT_TYPE.xContent(),
                new BytesArray(actual));
        QueryParseContext qsc = indexService.newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); })
                .newParseContext(sourceParser);
        assertThat(qsc.parseInnerQueryBuilder(), equalTo(expected));
    }

    public void testEmptyName() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("").field("type", "percolator").endObject().endObject()
            .endObject().endObject().string();
        DocumentMapperParser parser = mapperService.documentMapperParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type1", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testImplicitlySetDefaultScriptLang() throws Exception {
        addQueryFieldMappings();
        XContentBuilder query = jsonBuilder();
        query.startObject();
        query.startObject("script");
        if (randomBoolean()) {
            query.field("script", "return true");
        } else {
            query.startObject("script");
            query.field("inline", "return true");
            query.endObject();
        }
        query.endObject();
        query.endObject();

        ParsedDocument doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1",
                XContentFactory.jsonBuilder().startObject()
                        .rawField(fieldName, new BytesArray(query.string()), query.contentType())
                        .endObject().bytes(),
                        XContentType.JSON));
        BytesRef querySource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        Map<String, Object> parsedQuery = XContentHelper.convertToMap(new BytesArray(querySource), true).v2();
        assertEquals(Script.DEFAULT_SCRIPT_LANG, XContentMapValues.extractValue("script.script.lang", parsedQuery));

        query = jsonBuilder();
        query.startObject();
        query.startObject("function_score");
        query.startArray("functions");
        query.startObject();
        query.startObject("script_score");
        if (randomBoolean()) {
            query.field("script", "return true");
        } else {
            query.startObject("script");
            query.field("inline", "return true");
            query.endObject();
        }
        query.endObject();
        query.endObject();
        query.endArray();
        query.endObject();
        query.endObject();

        doc = mapperService.documentMapper("doc").parse(SourceToParse.source("test", "doc", "1",
                XContentFactory.jsonBuilder().startObject()
                        .rawField(fieldName, new BytesArray(query.string()), query.contentType())
                        .endObject().bytes(),
                        XContentType.JSON));
        querySource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        parsedQuery = XContentHelper.convertToMap(new BytesArray(querySource), true).v2();
        assertEquals(Script.DEFAULT_SCRIPT_LANG,
                ((List) XContentMapValues.extractValue("function_score.functions.script_score.script.lang", parsedQuery)).get(0));
    }

    // Just so that we store scripts in percolator queries, but not really execute these scripts.
    public static class FoolMeScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("return true", (vars) -> true);
        }

        @Override
        public String pluginScriptLang() {
            return Script.DEFAULT_SCRIPT_LANG;
        }
    }

}
