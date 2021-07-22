/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CoveringQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
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
    private PercolatorFieldMapper.PercolatorFieldType fieldType;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, PercolatorPlugin.class, FoolMeScriptPlugin.class, ParentJoinPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Before
    public void init() throws Exception {
        indexService = createIndex("test");
        mapperService = indexService.mapperService();

        String mapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("doc")
            .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .startObject("field1").field("type", "text").endObject()
                .startObject("field2").field("type", "text").endObject()
                .startObject("_field3").field("type", "text").endObject()
                .startObject("field4").field("type", "text").endObject()
                .startObject("number_field1").field("type", "integer").endObject()
                .startObject("number_field2").field("type", "long").endObject()
                .startObject("number_field3").field("type", "long").endObject()
                .startObject("number_field4").field("type", "half_float").endObject()
                .startObject("number_field5").field("type", "float").endObject()
                .startObject("number_field6").field("type", "double").endObject()
                .startObject("number_field7").field("type", "ip").endObject()
                .startObject("date_field").field("type", "date").endObject()
            .endObject().endObject().endObject());
        mapperService.merge("doc", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE);
    }

    private void addQueryFieldMappings() throws Exception {
        fieldName = randomAlphaOfLength(4);
        String percolatorMapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("doc")
                .startObject("properties").startObject(fieldName).field("type", "percolator").endObject().endObject()
                .endObject().endObject());
        mapperService.merge("doc", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE);
        fieldType = (PercolatorFieldMapper.PercolatorFieldType) mapperService.fieldType(fieldName);
    }

    public void testExtractTerms() throws Exception {
        addQueryFieldMappings();
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("field", "term1"));
        bq.add(termQuery1, Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("field", "term2"));
        bq.add(termQuery2, Occur.SHOULD);

        DocumentMapper documentMapper = mapperService.documentMapper();
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        DocumentParserContext documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(bq.build(), documentParserContext);
        LuceneDocument document = documentParserContext.doc();

        PercolatorFieldMapper.PercolatorFieldType fieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_COMPLETE));
        List<IndexableField> fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.queryTermsField.name())));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0).binaryValue().utf8ToString(), equalTo("field\u0000term1"));
        assertThat(fields.get(1).binaryValue().utf8ToString(), equalTo("field\u0000term2"));

        fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.minimumShouldMatchField.name())));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).numericValue(), equalTo(1L));

        // Now test conjunction:
        bq = new BooleanQuery.Builder();
        bq.add(termQuery1, Occur.MUST);
        bq.add(termQuery2, Occur.MUST);

        documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(bq.build(), documentParserContext);
        document = documentParserContext.doc();

        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_COMPLETE));
        fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.queryTermsField.name())));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0).binaryValue().utf8ToString(), equalTo("field\u0000term1"));
        assertThat(fields.get(1).binaryValue().utf8ToString(), equalTo("field\u0000term2"));

        fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.minimumShouldMatchField.name())));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).numericValue(), equalTo(2L));
    }

    public void testExtractRanges() throws Exception {
        SearchExecutionContext context = createSearchContext(indexService).getSearchExecutionContext();
        addQueryFieldMappings();
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        Query rangeQuery1 = mapperService.fieldType("number_field1")
            .rangeQuery(10, 20, true, true, null, null, null, context);
        bq.add(rangeQuery1, Occur.MUST);
        Query rangeQuery2 = mapperService.fieldType("number_field1")
            .rangeQuery(15, 20, true, true, null, null, null, context);
        bq.add(rangeQuery2, Occur.MUST);

        DocumentMapper documentMapper = mapperService.documentMapper();
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        DocumentParserContext documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(bq.build(), documentParserContext);
        LuceneDocument document = documentParserContext.doc();

        PercolatorFieldMapper.PercolatorFieldType fieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
        List<IndexableField> fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.rangeField.name())));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields.size(), equalTo(2));
        assertThat(IntPoint.decodeDimension(fields.get(0).binaryValue().bytes, 12), equalTo(10));
        assertThat(IntPoint.decodeDimension(fields.get(0).binaryValue().bytes, 28), equalTo(20));
        assertThat(IntPoint.decodeDimension(fields.get(1).binaryValue().bytes, 12), equalTo(15));
        assertThat(IntPoint.decodeDimension(fields.get(1).binaryValue().bytes, 28), equalTo(20));

        fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.minimumShouldMatchField.name())));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).numericValue(), equalTo(1L));

        // Range queries on different fields:
        bq = new BooleanQuery.Builder();
        bq.add(rangeQuery1, Occur.MUST);
        rangeQuery2 = mapperService.fieldType("number_field2")
            .rangeQuery(15, 20, true, true, null, null, null, context);
        bq.add(rangeQuery2, Occur.MUST);

        documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(bq.build(), documentParserContext);
        document = documentParserContext.doc();

        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
        fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.rangeField.name())));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields.size(), equalTo(2));
        assertThat(IntPoint.decodeDimension(fields.get(0).binaryValue().bytes, 12), equalTo(10));
        assertThat(IntPoint.decodeDimension(fields.get(0).binaryValue().bytes, 28), equalTo(20));
        assertThat(LongPoint.decodeDimension(fields.get(1).binaryValue().bytes, 8), equalTo(15L));
        assertThat(LongPoint.decodeDimension(fields.get(1).binaryValue().bytes, 24), equalTo(20L));

        fields = new ArrayList<>(Arrays.asList(document.getFields(fieldType.minimumShouldMatchField.name())));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).numericValue(), equalTo(2L));
    }

    public void testExtractTermsAndRanges_failed() throws Exception {
        addQueryFieldMappings();
        TermRangeQuery query = new TermRangeQuery("field1", new BytesRef("a"), new BytesRef("z"), true, true);
        DocumentMapper documentMapper = mapperService.documentMapper();
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        DocumentParserContext documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(query, documentParserContext);
        LuceneDocument document = documentParserContext.doc();

        PercolatorFieldMapper.PercolatorFieldType fieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getFields().size(), equalTo(1));
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_FAILED));
    }

    public void testExtractTermsAndRanges_partial() throws Exception {
        addQueryFieldMappings();
        PhraseQuery phraseQuery = new PhraseQuery("field", "term");
        DocumentMapper documentMapper = mapperService.documentMapper();
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        DocumentParserContext documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(phraseQuery, documentParserContext);
        LuceneDocument document = documentParserContext.doc();

        PercolatorFieldMapper.PercolatorFieldType fieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getFields().size(), equalTo(3));
        assertThat(document.getFields().get(0).binaryValue().utf8ToString(), equalTo("field\u0000term"));
        assertThat(document.getField(fieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
    }

    public void testExtractTermsAndRanges() throws Exception {
        addQueryFieldMappings();

        MemoryIndex memoryIndex = new MemoryIndex(false);
        memoryIndex.addField("field1", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        memoryIndex.addField("field2", "some more text", new WhitespaceAnalyzer());
        memoryIndex.addField("_field3", "unhide me", new WhitespaceAnalyzer());
        memoryIndex.addField("field4", "123", new WhitespaceAnalyzer());
        memoryIndex.addField(new LongPoint("number_field2", 10L), new WhitespaceAnalyzer());

        IndexReader indexReader = memoryIndex.createSearcher().getIndexReader();

        Tuple<List<BytesRef>, Map<String, List<byte[]>>> t = fieldType.extractTermsAndRanges(indexReader);
        assertEquals(1, t.v2().size());
        Map<String, List<byte[]>> rangesMap = t.v2();
        assertEquals(1, rangesMap.size());

        List<byte[]> range = rangesMap.get("number_field2");
        assertNotNull(range);
        assertEquals(10, LongPoint.decodeDimension(range.get(0), 0));
        assertEquals(10, LongPoint.decodeDimension(range.get(1), 0));

        List<BytesRef> terms = t.v1();
        terms.sort(BytesRef::compareTo);
        assertEquals(14, terms.size());
        assertEquals("_field3\u0000me", terms.get(0).utf8ToString());
        assertEquals("_field3\u0000unhide", terms.get(1).utf8ToString());
        assertEquals("field1\u0000brown", terms.get(2).utf8ToString());
        assertEquals("field1\u0000dog", terms.get(3).utf8ToString());
        assertEquals("field1\u0000fox", terms.get(4).utf8ToString());
        assertEquals("field1\u0000jumps", terms.get(5).utf8ToString());
        assertEquals("field1\u0000lazy", terms.get(6).utf8ToString());
        assertEquals("field1\u0000over", terms.get(7).utf8ToString());
        assertEquals("field1\u0000quick", terms.get(8).utf8ToString());
        assertEquals("field1\u0000the", terms.get(9).utf8ToString());
        assertEquals("field2\u0000more", terms.get(10).utf8ToString());
        assertEquals("field2\u0000some", terms.get(11).utf8ToString());
        assertEquals("field2\u0000text", terms.get(12).utf8ToString());
        assertEquals("field4\u0000123", terms.get(13).utf8ToString());
    }


    public void testCreateCandidateQuery() throws Exception {
        addQueryFieldMappings();

        MemoryIndex memoryIndex = new MemoryIndex(false);
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 4094; i++) {
            text.append(i).append(' ');
        }
        memoryIndex.addField("field1", text.toString(), new WhitespaceAnalyzer());
        memoryIndex.addField(new LongPoint("field2", 10L), new WhitespaceAnalyzer());
        IndexReader indexReader = memoryIndex.createSearcher().getIndexReader();

        Tuple<BooleanQuery, Boolean> t = fieldType.createCandidateQuery(indexReader, Version.CURRENT);
        assertTrue(t.v2());
        assertEquals(2, t.v1().clauses().size());
        assertThat(t.v1().clauses().get(0).getQuery(), instanceOf(CoveringQuery.class));
        assertThat(t.v1().clauses().get(1).getQuery(), instanceOf(TermQuery.class));

        // Now push it over the edge, so that it falls back using TermInSetQuery
        memoryIndex.addField("field2", "value", new WhitespaceAnalyzer());
        indexReader = memoryIndex.createSearcher().getIndexReader();
        t = fieldType.createCandidateQuery(indexReader, Version.CURRENT);
        assertFalse(t.v2());
        assertEquals(3, t.v1().clauses().size());
        TermInSetQuery terms = (TermInSetQuery) t.v1().clauses().get(0).getQuery();
        assertEquals(4095, terms.getTermData().size());
        assertThat(t.v1().clauses().get(1).getQuery().toString(), containsString(fieldName + ".range_field:<ranges:"));
        assertThat(t.v1().clauses().get(2).getQuery().toString(), containsString(fieldName + ".extraction_result:failed"));
    }

    public void testExtractTermsAndRanges_numberFields() throws Exception {
        addQueryFieldMappings();

        MemoryIndex memoryIndex = new MemoryIndex(false);
        memoryIndex.addField(new IntPoint("number_field1", 10), new WhitespaceAnalyzer());
        memoryIndex.addField(new LongPoint("number_field2", 20L), new WhitespaceAnalyzer());
        memoryIndex.addField(new LongPoint("number_field3", 30L), new WhitespaceAnalyzer());
        memoryIndex.addField(new HalfFloatPoint("number_field4", 30f), new WhitespaceAnalyzer());
        memoryIndex.addField(new FloatPoint("number_field5", 40f), new WhitespaceAnalyzer());
        memoryIndex.addField(new DoublePoint("number_field6", 50f), new WhitespaceAnalyzer());
        memoryIndex.addField(new InetAddressPoint("number_field7", InetAddresses.forString("192.168.1.12")), new WhitespaceAnalyzer());
        memoryIndex.addField(new InetAddressPoint("number_field7", InetAddresses.forString("192.168.1.20")), new WhitespaceAnalyzer());
        memoryIndex.addField(new InetAddressPoint("number_field7", InetAddresses.forString("192.168.1.24")), new WhitespaceAnalyzer());

        IndexReader indexReader = memoryIndex.createSearcher().getIndexReader();

        Tuple<List<BytesRef>, Map<String, List<byte[]>>> t = fieldType.extractTermsAndRanges(indexReader);
        assertEquals(0, t.v1().size());
        Map<String, List<byte[]>> rangesMap = t.v2();
        assertEquals(7, rangesMap.size());

        List<byte[]> range = rangesMap.get("number_field1");
        assertNotNull(range);
        assertEquals(10, IntPoint.decodeDimension(range.get(0), 0));
        assertEquals(10, IntPoint.decodeDimension(range.get(1), 0));

        range = rangesMap.get("number_field2");
        assertNotNull(range);
        assertEquals(20L, LongPoint.decodeDimension(range.get(0), 0));
        assertEquals(20L, LongPoint.decodeDimension(range.get(1), 0));

        range = rangesMap.get("number_field3");
        assertNotNull(range);
        assertEquals(30L, LongPoint.decodeDimension(range.get(0), 0));
        assertEquals(30L, LongPoint.decodeDimension(range.get(1), 0));

        range = rangesMap.get("number_field4");
        assertNotNull(range);
        assertEquals(30F, HalfFloatPoint.decodeDimension(range.get(0), 0), 0F);
        assertEquals(30F, HalfFloatPoint.decodeDimension(range.get(1), 0), 0F);

        range = rangesMap.get("number_field5");
        assertNotNull(range);
        assertEquals(40F, FloatPoint.decodeDimension(range.get(0), 0), 0F);
        assertEquals(40F, FloatPoint.decodeDimension(range.get(1), 0), 0F);

        range = rangesMap.get("number_field6");
        assertNotNull(range);
        assertEquals(50D, DoublePoint.decodeDimension(range.get(0), 0), 0D);
        assertEquals(50D, DoublePoint.decodeDimension(range.get(1), 0), 0D);

        range = rangesMap.get("number_field7");
        assertNotNull(range);
        assertEquals(InetAddresses.forString("192.168.1.12"), InetAddressPoint.decode(range.get(0)));
        assertEquals(InetAddresses.forString("192.168.1.24"), InetAddressPoint.decode(range.get(1)));
    }

    public void testPercolatorFieldMapper() throws Exception {
        addQueryFieldMappings();
        QueryBuilder queryBuilder = termQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                        BytesReference.bytes(XContentFactory
                                .jsonBuilder()
                                .startObject()
                                .field(fieldName, queryBuilder)
                                .endObject()),
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
        doc = mapperService.documentMapper().parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory
                .jsonBuilder()
                .startObject()
                .field(fieldName, queryBuilder)
                .endObject()),
                XContentType.JSON));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name())[0].stringValue(),
                equalTo(EXTRACTION_FAILED));
        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name()).length, equalTo(0));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()).length, equalTo(1));
        qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);

        queryBuilder = rangeQuery("date_field").from("now");
        doc = mapperService.documentMapper().parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory
            .jsonBuilder()
            .startObject()
            .field(fieldName, queryBuilder)
            .endObject()),
            XContentType.JSON));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name())[0].stringValue(),
            equalTo(EXTRACTION_FAILED));
    }

    public void testStoringQueries() throws Exception {
        addQueryFieldMappings();
        QueryBuilder[] queries = new QueryBuilder[]{
                termQuery("field", "value"), matchAllQuery(), matchQuery("field", "value"), matchPhraseQuery("field", "value"),
                prefixQuery("field", "v"), wildcardQuery("field", "v*"), rangeQuery("number_field2").gte(0).lte(9),
                rangeQuery("date_field").from("2015-01-01T00:00").to("2015-01-01T00:00")
        };
        // note: it important that range queries never rewrite, otherwise it will cause results to be wrong.
        // (it can't use shard data for rewriting purposes, because percolator queries run on MemoryIndex)

        for (QueryBuilder query : queries) {
            ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                    .field(fieldName, query)
                    .endObject()),
                    XContentType.JSON));
            BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
            assertQueryBuilder(qbSource, query);
        }
    }

    public void testQueryWithRewrite() throws Exception {
        addQueryFieldMappings();
        client().prepareIndex("remote").setId("1").setSource("field", "value").get();
        QueryBuilder queryBuilder = termsLookupQuery("field", new TermsLookup("remote", "1", "field"));
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                        BytesReference.bytes(XContentFactory
                                .jsonBuilder()
                                .startObject()
                                .field(fieldName, queryBuilder)
                                .endObject()),
                        XContentType.JSON));
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> {
                throw new UnsupportedOperationException();
            }, null, emptyMap());
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(queryBuilder, searchExecutionContext, future);
        assertQueryBuilder(qbSource, future.get());
    }


    public void testPercolatorFieldMapperUnMappedField() throws Exception {
        addQueryFieldMappings();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            mapperService.documentMapper().parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .field(fieldName, termQuery("unmapped_field", "value"))
                    .endObject()),
                    XContentType.JSON));
        });
        assertThat(exception.getCause(), instanceOf(QueryShardException.class));
        assertThat(exception.getCause().getMessage(), equalTo("No field mapping can be found for the field with name [unmapped_field]"));
    }


    public void testPercolatorFieldMapper_noQuery() throws Exception {
        addQueryFieldMappings();
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory
                        .jsonBuilder()
                        .startObject()
                        .endObject()),
                XContentType.JSON));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()).length, equalTo(0));

        try {
            mapperService.documentMapper().parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .nullField(fieldName)
                    .endObject()),
                    XContentType.JSON));
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("query malformed, must start with start_object"));
        }
    }

    public void testAllowNoAdditionalSettings() throws Exception {
        addQueryFieldMappings();
        IndexService indexService = createIndex("test1", Settings.EMPTY);
        MapperService mapperService = indexService.mapperService();

        String percolatorMapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("doc")
            .startObject("properties").startObject(fieldName).field("type", "percolator").field("index", "no").endObject().endObject()
            .endObject().endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapperService.merge("doc", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("Mapping definition for [" + fieldName + "] has unsupported parameters:  [index : no]"));
    }

    // multiple percolator fields are allowed in the mapping, but only one field can be used at index time.
    public void testMultiplePercolatorFields() throws Exception {
        String typeName = "doc";
        String percolatorMapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("properties")
                    .startObject("query_field1").field("type", "percolator").endObject()
                    .startObject("query_field2").field("type", "percolator").endObject()
                .endObject()
                .endObject().endObject());
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(jsonBuilder().startObject()
                        .field("query_field1", queryBuilder)
                        .field("query_field2", queryBuilder)
                        .endObject()),
                        XContentType.JSON));
        assertThat(doc.rootDoc().getFields().size(), equalTo(16)); // also includes all other meta fields
        BytesRef queryBuilderAsBytes = doc.rootDoc().getField("query_field1.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        queryBuilderAsBytes = doc.rootDoc().getField("query_field2.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);
    }

    // percolator field can be nested under an object field, but only one query can be specified per document
    public void testNestedPercolatorField() throws Exception {
        String typeName = "doc";
        String percolatorMapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("properties")
                .startObject("object_field")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("query_field").field("type", "percolator").endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject());
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(jsonBuilder().startObject().startObject("object_field")
                            .field("query_field", queryBuilder)
                        .endObject().endObject()),
                        XContentType.JSON));
        assertThat(doc.rootDoc().getFields().size(), equalTo(11)); // also includes all other meta fields
        IndexableField queryBuilderField = doc.rootDoc().getField("object_field.query_field.query_builder_field");
        assertTrue(queryBuilderField.fieldType().omitNorms());
        IndexableField extractionResultField = doc.rootDoc().getField("object_field.query_field.extraction_result");
        assertTrue(extractionResultField.fieldType().omitNorms());
        BytesRef queryBuilderAsBytes = queryBuilderField.binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(jsonBuilder().startObject()
                            .startArray("object_field")
                                .startObject().field("query_field", queryBuilder).endObject()
                            .endArray()
                        .endObject()),
                        XContentType.JSON));
        assertThat(doc.rootDoc().getFields().size(), equalTo(11)); // also includes all other meta fields
        queryBuilderAsBytes = doc.rootDoc().getField("object_field.query_field.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
                    mapperService.documentMapper().parse(new SourceToParse("test", "1",
                            BytesReference.bytes(jsonBuilder().startObject()
                                    .startArray("object_field")
                                        .startObject().field("query_field", queryBuilder).endObject()
                                        .startObject().field("query_field", queryBuilder).endObject()
                                    .endArray()
                                .endObject()),
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

        HasChildQueryBuilder hasChildQuery = new HasChildQueryBuilder("parent", new MatchAllQueryBuilder(), ScoreMode.None);
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

        HasParentQueryBuilder hasParentQuery = new HasParentQueryBuilder("parent", new MatchAllQueryBuilder(), false);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.verifyQuery(hasParentQuery));
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.verifyQuery(new BoolQueryBuilder().must(hasParentQuery)));
    }

    private void assertQueryBuilder(BytesRef actual, QueryBuilder expected) throws IOException {
        try (InputStream in = new ByteArrayInputStream(actual.bytes, actual.offset, actual.length)) {
            try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), writableRegistry())) {
                // Query builder's content is stored via BinaryFieldMapper, which has a custom encoding
                // to encode multiple binary values into a single binary doc values field.
                // This is the reason we need to first need to read the number of values and
                // then the length of the field value in bytes.
                input.readVInt();
                input.readVInt();
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                assertThat(queryBuilder, equalTo(expected));
            }
        }
    }

    public void testEmptyName() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("").field("type", "percolator").endObject().endObject()
            .endObject().endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> mapperService.parseMapping("type1", new CompressedXContent(mapping))
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
            query.field("source", "return true");
            query.endObject();
        }
        query.endObject();
        query.endObject();

        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                        .rawField(fieldName, new BytesArray(Strings.toString(query)).streamInput(), query.contentType())
                        .endObject()),
                        XContentType.JSON));
        BytesRef querySource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        try (InputStream in = new ByteArrayInputStream(querySource.bytes, querySource.offset, querySource.length)) {
            try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), writableRegistry())) {
                // Query builder's content is stored via BinaryFieldMapper, which has a custom encoding
                // to encode multiple binary values into a single binary doc values field.
                // This is the reason we need to first need to read the number of values and
                // then the length of the field value in bytes.
                input.readVInt();
                input.readVInt();
                ScriptQueryBuilder queryBuilder = (ScriptQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
                assertEquals(Script.DEFAULT_SCRIPT_LANG, queryBuilder.script().getLang());
            }
        }

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
            query.field("source", "return true");
            query.endObject();
        }
        query.endObject();
        query.endObject();
        query.endArray();
        query.endObject();
        query.endObject();

        doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                        .rawField(fieldName, new BytesArray(Strings.toString(query)).streamInput(), query.contentType())
                        .endObject()),
                        XContentType.JSON));
        querySource = doc.rootDoc().getFields(fieldType.queryBuilderField.name())[0].binaryValue();
        try (InputStream in = new ByteArrayInputStream(querySource.bytes, querySource.offset, querySource.length)) {
            try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), writableRegistry())) {
                input.readVInt();
                input.readVInt();
                FunctionScoreQueryBuilder queryBuilder = (FunctionScoreQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
                ScriptScoreFunctionBuilder function = (ScriptScoreFunctionBuilder)
                    queryBuilder.filterFunctionBuilders()[0].getScoreFunction();
                assertEquals(Script.DEFAULT_SCRIPT_LANG, function.getScript().getLang());
            }
        }
    }

    public void testEncodeRange() {
        int iters = randomIntBetween(32, 256);
        for (int i = 0; i < iters; i++) {
            int encodingType = randomInt(1);

            final int randomFrom = randomInt();
            final byte[] encodedFrom;
            switch (encodingType) {
                case 0:
                    encodedFrom = new byte[Integer.BYTES];
                    IntPoint.encodeDimension(randomFrom, encodedFrom, 0);
                    break;
                case 1:
                    encodedFrom = new byte[Long.BYTES];
                    LongPoint.encodeDimension(randomFrom, encodedFrom, 0);
                    break;
                default:
                    throw new AssertionError("unexpected encoding type [" + encodingType + "]");
            }

            final int randomTo = randomIntBetween(randomFrom, Integer.MAX_VALUE);
            final byte[] encodedTo;
            switch (encodingType) {
                case 0:
                    encodedTo = new byte[Integer.BYTES];
                    IntPoint.encodeDimension(randomTo, encodedTo, 0);
                    break;
                case 1:
                    encodedTo = new byte[Long.BYTES];
                    LongPoint.encodeDimension(randomTo, encodedTo, 0);
                    break;
                default:
                    throw new AssertionError("unexpected encoding type [" + encodingType + "]");
            }

            String fieldName = randomAlphaOfLength(5);
            byte[] result = PercolatorFieldMapper.encodeRange(fieldName, encodedFrom, encodedTo);
            assertEquals(32, result.length);

            BytesRef fieldAsBytesRef = new BytesRef(fieldName);
            MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
            MurmurHash3.hash128(fieldAsBytesRef.bytes, fieldAsBytesRef.offset, fieldAsBytesRef.length, 0, hash);

            switch (encodingType) {
                case 0:
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 0, 8)).getLong());
                    assertEquals(randomFrom, IntPoint.decodeDimension(subByteArray(result, 12, 4), 0));
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 16, 8)).getLong());
                    assertEquals(randomTo, IntPoint.decodeDimension(subByteArray(result, 28, 4), 0));
                    break;
                case 1:
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 0, 8)).getLong());
                    assertEquals(randomFrom, LongPoint.decodeDimension(subByteArray(result, 8, 8), 0));
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 16, 8)).getLong());
                    assertEquals(randomTo, LongPoint.decodeDimension(subByteArray(result, 24, 8), 0));
                    break;
                default:
                    throw new AssertionError("unexpected encoding type [" + encodingType + "]");
            }
        }
    }

    public void testDuplicatedClauses() throws Exception {
        addQueryFieldMappings();

        QueryBuilder qb = boolQuery()
                .must(boolQuery().must(termQuery("field", "value1")).must(termQuery("field", "value2")))
                .must(boolQuery().must(termQuery("field", "value2")).must(termQuery("field", "value3")));
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                        .field(fieldName, qb)
                        .endObject()),
                XContentType.JSON));

        List<String> values = Arrays.stream(doc.rootDoc().getFields(fieldType.queryTermsField.name()))
                .map(f -> f.binaryValue().utf8ToString())
                .sorted()
                .collect(Collectors.toList());
        assertThat(values.size(), equalTo(3));
        assertThat(values.get(0), equalTo("field\0value1"));
        assertThat(values.get(1), equalTo("field\0value2"));
        assertThat(values.get(2), equalTo("field\0value3"));
        int msm = doc.rootDoc().getFields(fieldType.minimumShouldMatchField.name())[0].numericValue().intValue();
        assertThat(msm, equalTo(3));

        qb = boolQuery()
                .must(boolQuery().must(termQuery("field", "value1")).must(termQuery("field", "value2")))
                .must(boolQuery().must(termQuery("field", "value2")).must(termQuery("field", "value3")))
                .must(boolQuery().must(termQuery("field", "value3")).must(termQuery("field", "value4")))
                .must(boolQuery().should(termQuery("field", "value4")).should(termQuery("field", "value5")));
        doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                        .field(fieldName, qb)
                        .endObject()),
                XContentType.JSON));

        values = Arrays.stream(doc.rootDoc().getFields(fieldType.queryTermsField.name()))
                .map(f -> f.binaryValue().utf8ToString())
                .sorted()
                .collect(Collectors.toList());
        assertThat(values.size(), equalTo(5));
        assertThat(values.get(0), equalTo("field\0value1"));
        assertThat(values.get(1), equalTo("field\0value2"));
        assertThat(values.get(2), equalTo("field\0value3"));
        assertThat(values.get(3), equalTo("field\0value4"));
        assertThat(values.get(4), equalTo("field\0value5"));
        msm = doc.rootDoc().getFields(fieldType.minimumShouldMatchField.name())[0].numericValue().intValue();
        assertThat(msm, equalTo(4));

        qb = boolQuery()
                .minimumShouldMatch(3)
                .should(boolQuery().should(termQuery("field", "value1")).should(termQuery("field", "value2")))
                .should(boolQuery().should(termQuery("field", "value2")).should(termQuery("field", "value3")))
                .should(boolQuery().should(termQuery("field", "value3")).should(termQuery("field", "value4")))
                .should(boolQuery().should(termQuery("field", "value4")).should(termQuery("field", "value5")));
        doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                        .field(fieldName, qb)
                        .endObject()),
                XContentType.JSON));

        values = Arrays.stream(doc.rootDoc().getFields(fieldType.queryTermsField.name()))
                .map(f -> f.binaryValue().utf8ToString())
                .sorted()
                .collect(Collectors.toList());
        assertThat(values.size(), equalTo(5));
        assertThat(values.get(0), equalTo("field\0value1"));
        assertThat(values.get(1), equalTo("field\0value2"));
        assertThat(values.get(2), equalTo("field\0value3"));
        assertThat(values.get(3), equalTo("field\0value4"));
        assertThat(values.get(4), equalTo("field\0value5"));
        msm = doc.rootDoc().getFields(fieldType.minimumShouldMatchField.name())[0].numericValue().intValue();
        assertThat(msm, equalTo(1));
    }

    private static byte[] subByteArray(byte[] source, int offset, int length) {
        return Arrays.copyOfRange(source, offset, offset + length);
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
