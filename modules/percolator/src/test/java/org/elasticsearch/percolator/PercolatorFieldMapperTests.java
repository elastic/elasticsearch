/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.index.query.AbstractQueryBuilder;
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
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DummyQueryParserPlugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
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
import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

//TODO migrate tests that don't require a node to a unit test that subclasses MapperTestCase
public class PercolatorFieldMapperTests extends ESSingleNodeTestCase {

    private String fieldName;
    private IndexService indexService;
    private MapperService mapperService;
    private PercolatorFieldMapper.PercolatorFieldType fieldType;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            InternalSettingsPlugin.class,
            PercolatorPlugin.class,
            FoolMeScriptPlugin.class,
            ParentJoinPlugin.class,
            CustomQueriesPlugin.class
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Before
    public void init() throws Exception {
        indexService = createIndex("test");
        mapperService = indexService.mapperService();

        String mapper = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "text")
                .endObject()
                .startObject("field1")
                .field("type", "text")
                .endObject()
                .startObject("field2")
                .field("type", "text")
                .endObject()
                .startObject("_field3")
                .field("type", "text")
                .endObject()
                .startObject("field4")
                .field("type", "text")
                .endObject()
                .startObject("number_field1")
                .field("type", "integer")
                .endObject()
                .startObject("number_field2")
                .field("type", "long")
                .endObject()
                .startObject("number_field3")
                .field("type", "long")
                .endObject()
                .startObject("number_field4")
                .field("type", "half_float")
                .endObject()
                .startObject("number_field5")
                .field("type", "float")
                .endObject()
                .startObject("number_field6")
                .field("type", "double")
                .endObject()
                .startObject("number_field7")
                .field("type", "ip")
                .endObject()
                .startObject("date_field")
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        mapperService.merge("doc", new CompressedXContent(mapper), MergeReason.MAPPING_UPDATE);
    }

    private void addQueryFieldMappings() throws Exception {
        fieldName = randomAlphaOfLength(4);
        String percolatorMapper = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("doc")
                .startObject("properties")
                .startObject(fieldName)
                .field("type", "percolator")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        mapperService.merge("doc", new CompressedXContent(percolatorMapper), MergeReason.MAPPING_UPDATE);
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

        PercolatorFieldMapper.PercolatorFieldType percolatorFieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getField(percolatorFieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_COMPLETE));
        List<IndexableField> fields = new ArrayList<>(document.getFields(percolatorFieldType.queryTermsField.name()));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields, transformedItemsMatch(b -> b.binaryValue().utf8ToString(), contains("field\u0000term1", "field\u0000term2")));

        fields = new ArrayList<>(document.getFields(percolatorFieldType.minimumShouldMatchField.name()));
        assertThat(fields, transformedItemsMatch(IndexableField::numericValue, contains(1L)));

        // Now test conjunction:
        bq = new BooleanQuery.Builder();
        bq.add(termQuery1, Occur.MUST);
        bq.add(termQuery2, Occur.MUST);

        documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(bq.build(), documentParserContext);
        document = documentParserContext.doc();

        assertThat(document.getField(percolatorFieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_COMPLETE));
        fields = new ArrayList<>(document.getFields(percolatorFieldType.queryTermsField.name()));
        fields.sort(Comparator.comparing(IndexableField::binaryValue));
        assertThat(fields, transformedItemsMatch(b -> b.binaryValue().utf8ToString(), contains("field\u0000term1", "field\u0000term2")));

        fields = new ArrayList<>(document.getFields(percolatorFieldType.minimumShouldMatchField.name()));
        assertThat(fields, transformedItemsMatch(IndexableField::numericValue, contains(2L)));
    }

    public void testExtractRanges() throws Exception {
        try (SearchContext searchContext = createSearchContext(indexService)) {
            SearchExecutionContext context = searchContext.getSearchExecutionContext();
            addQueryFieldMappings();
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            Query rangeQuery1 = mapperService.fieldType("number_field1").rangeQuery(10, 20, true, true, null, null, null, context);
            bq.add(rangeQuery1, Occur.MUST);
            Query rangeQuery2 = mapperService.fieldType("number_field1").rangeQuery(15, 20, true, true, null, null, null, context);
            bq.add(rangeQuery2, Occur.MUST);

            DocumentMapper documentMapper = mapperService.documentMapper();
            PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
            DocumentParserContext documentParserContext = new TestDocumentParserContext();
            fieldMapper.processQuery(bq.build(), documentParserContext);
            LuceneDocument document = documentParserContext.doc();

            PercolatorFieldMapper.PercolatorFieldType percolatorFieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper
                .fieldType();
            assertThat(document.getField(percolatorFieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
            List<IndexableField> fields = new ArrayList<>(document.getFields(percolatorFieldType.rangeField.name()));
            fields.sort(Comparator.comparing(IndexableField::binaryValue));
            assertThat(
                fields,
                transformedItemsMatch(
                    b -> b.binaryValue().bytes,
                    contains(
                        allOf(
                            transformedMatch(b -> IntPoint.decodeDimension(b, 12), equalTo(10)),
                            transformedMatch(b -> IntPoint.decodeDimension(b, 28), equalTo(20))
                        ),
                        allOf(
                            transformedMatch(b -> IntPoint.decodeDimension(b, 12), equalTo(15)),
                            transformedMatch(b -> IntPoint.decodeDimension(b, 28), equalTo(20))
                        )
                    )
                )
            );

            fields = new ArrayList<>(document.getFields(percolatorFieldType.minimumShouldMatchField.name()));
            assertThat(fields, transformedItemsMatch(IndexableField::numericValue, contains(1L)));

            // Range queries on different fields:
            bq = new BooleanQuery.Builder();
            bq.add(rangeQuery1, Occur.MUST);
            rangeQuery2 = mapperService.fieldType("number_field2").rangeQuery(15, 20, true, true, null, null, null, context);
            bq.add(rangeQuery2, Occur.MUST);

            documentParserContext = new TestDocumentParserContext();
            fieldMapper.processQuery(bq.build(), documentParserContext);
            document = documentParserContext.doc();

            assertThat(document.getField(percolatorFieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
            fields = new ArrayList<>(document.getFields(percolatorFieldType.rangeField.name()));
            fields.sort(Comparator.comparing(IndexableField::binaryValue));
            assertThat(
                fields,
                transformedItemsMatch(
                    b -> b.binaryValue().bytes,
                    contains(
                        allOf(
                            transformedMatch(b -> IntPoint.decodeDimension(b, 12), equalTo(10)),
                            transformedMatch(b -> IntPoint.decodeDimension(b, 28), equalTo(20))
                        ),
                        allOf(
                            transformedMatch(b -> LongPoint.decodeDimension(b, 8), equalTo(15L)),
                            transformedMatch(b -> LongPoint.decodeDimension(b, 24), equalTo(20L))
                        )
                    )
                )
            );

            fields = new ArrayList<>(document.getFields(percolatorFieldType.minimumShouldMatchField.name()));
            assertThat(fields, transformedItemsMatch(IndexableField::numericValue, contains(2L)));
        }
    }

    public void testExtractTermsAndRanges_failed() throws Exception {
        addQueryFieldMappings();
        TermRangeQuery query = new TermRangeQuery("field1", new BytesRef("a"), new BytesRef("z"), true, true);
        DocumentMapper documentMapper = mapperService.documentMapper();
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        DocumentParserContext documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(query, documentParserContext);
        LuceneDocument document = documentParserContext.doc();

        PercolatorFieldMapper.PercolatorFieldType percolatorFieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getFields().size(), equalTo(1));
        assertThat(document.getField(percolatorFieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_FAILED));
    }

    public void testExtractTermsAndRanges_partial() throws Exception {
        addQueryFieldMappings();
        PhraseQuery phraseQuery = new PhraseQuery("field", "term");
        DocumentMapper documentMapper = mapperService.documentMapper();
        PercolatorFieldMapper fieldMapper = (PercolatorFieldMapper) documentMapper.mappers().getMapper(fieldName);
        DocumentParserContext documentParserContext = new TestDocumentParserContext();
        fieldMapper.processQuery(phraseQuery, documentParserContext);
        LuceneDocument document = documentParserContext.doc();

        PercolatorFieldMapper.PercolatorFieldType percolatorFieldType = (PercolatorFieldMapper.PercolatorFieldType) fieldMapper.fieldType();
        assertThat(document.getFields().size(), equalTo(3));
        assertThat(document.getFields().get(0).binaryValue().utf8ToString(), equalTo("field\u0000term"));
        assertThat(document.getField(percolatorFieldType.extractionResultField.name()).stringValue(), equalTo(EXTRACTION_PARTIAL));
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

        Tuple<List<BytesRef>, Map<String, List<byte[]>>> t = PercolatorFieldMapper.PercolatorFieldType.extractTermsAndRanges(indexReader);
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
        int origMaxClauseCount = IndexSearcher.getMaxClauseCount();
        try {
            final int maxClauseCount = 100;
            IndexSearcher.setMaxClauseCount(maxClauseCount);
            addQueryFieldMappings();

            MemoryIndex memoryIndex = new MemoryIndex(false);
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < maxClauseCount - 2; i++) {
                text.append(i).append(' ');
            }
            memoryIndex.addField("field1", text.toString(), new WhitespaceAnalyzer());
            memoryIndex.addField(new LongPoint("field2", 10L), new WhitespaceAnalyzer());
            IndexReader indexReader = memoryIndex.createSearcher().getIndexReader();

            Tuple<BooleanQuery, Boolean> t = fieldType.createCandidateQuery(indexReader);
            assertTrue(t.v2());
            assertEquals(2, t.v1().clauses().size());
            assertThat(t.v1().clauses().get(0).query(), instanceOf(CoveringQuery.class));
            assertThat(t.v1().clauses().get(1).query(), instanceOf(TermQuery.class));

            // Now push it over the edge, so that it falls back using TermInSetQuery
            memoryIndex.addField("field2", "value", new WhitespaceAnalyzer());
            indexReader = memoryIndex.createSearcher().getIndexReader();
            t = fieldType.createCandidateQuery(indexReader);
            assertFalse(t.v2());
            assertEquals(3, t.v1().clauses().size());
            TermInSetQuery terms = (TermInSetQuery) t.v1().clauses().get(0).query();
            assertEquals(maxClauseCount - 1, terms.getTermsCount());
            assertThat(t.v1().clauses().get(1).query().toString(), containsString(fieldName + ".range_field:<ranges:"));
            assertThat(t.v1().clauses().get(2).query().toString(), containsString(fieldName + ".extraction_result:failed"));
        } finally {
            IndexSearcher.setMaxClauseCount(origMaxClauseCount);
        }
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

        Tuple<List<BytesRef>, Map<String, List<byte[]>>> t = PercolatorFieldMapper.PercolatorFieldType.extractTermsAndRanges(indexReader);
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
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, queryBuilder).endObject()),
                    XContentType.JSON
                )
            );

        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name()), hasSize(1));
        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name()).get(0).binaryValue().utf8ToString(), equalTo("field\0value"));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()), hasSize(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()), hasSize(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).get(0).stringValue(), equalTo(EXTRACTION_COMPLETE));
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name()).get(0).binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);

        // add an query for which we don't extract terms from
        queryBuilder = rangeQuery("field").from("a").to("z");
        doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, queryBuilder).endObject()),
                    XContentType.JSON
                )
            );
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()), hasSize(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).get(0).stringValue(), equalTo(EXTRACTION_FAILED));
        assertThat(doc.rootDoc().getFields(fieldType.queryTermsField.name()), hasSize(0));
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()), hasSize(1));
        qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name()).get(0).binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);

        queryBuilder = rangeQuery("date_field").from("now");
        doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, queryBuilder).endObject()),
                    XContentType.JSON
                )
            );
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()), hasSize(1));
        assertThat(doc.rootDoc().getFields(fieldType.extractionResultField.name()).get(0).stringValue(), equalTo(EXTRACTION_FAILED));
    }

    public void testParseScriptScoreQueryWithParams() throws Exception {
        addQueryFieldMappings();
        ScriptScoreQueryBuilder scriptScoreQueryBuilder = new ScriptScoreQueryBuilder(
            new MatchAllQueryBuilder(),
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "score", Collections.singletonMap("param", "1"))
        );
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, scriptScoreQueryBuilder).endObject()),
                    XContentType.JSON
                )
            );
        assertNotNull(doc);
    }

    public void testParseCustomParserQuery() throws Exception {
        addQueryFieldMappings();
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field(fieldName, new CustomParserQueryBuilder()).endObject()
                    ),
                    XContentType.JSON
                )
            );
        assertNotNull(doc);
    }

    public void testStoringQueries() throws Exception {
        addQueryFieldMappings();
        QueryBuilder[] queries = new QueryBuilder[] {
            termQuery("field", "value"),
            matchAllQuery(),
            matchQuery("field", "value"),
            matchPhraseQuery("field", "value"),
            prefixQuery("field", "v"),
            wildcardQuery("field", "v*"),
            rangeQuery("number_field2").gte(0).lte(9),
            rangeQuery("date_field").from("2015-01-01T00:00").to("2015-01-01T00:00") };
        // note: it important that range queries never rewrite, otherwise it will cause results to be wrong.
        // (it can't use shard data for rewriting purposes, because percolator queries run on MemoryIndex)

        for (QueryBuilder query : queries) {
            ParsedDocument doc = mapperService.documentMapper()
                .parse(
                    new SourceToParse(
                        "1",
                        BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, query).endObject()),
                        XContentType.JSON
                    )
                );
            BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name()).get(0).binaryValue();
            assertQueryBuilder(qbSource, query);
        }
    }

    public void testQueryWithRewrite() throws Exception {
        addQueryFieldMappings();
        prepareIndex("remote").setId("1").setSource("field", "value").get();
        QueryBuilder queryBuilder = termsLookupQuery("field", new TermsLookup("remote", "1", "field"));
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, queryBuilder).endObject()),
                    XContentType.JSON
                )
            );
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.queryBuilderField.name()).get(0).binaryValue();
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(randomInt(20), 0, null, () -> {
            throw new UnsupportedOperationException();
        }, null, emptyMap());
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(queryBuilder, searchExecutionContext, future);
        assertQueryBuilder(qbSource, future.get());
    }

    public void testPercolatorFieldMapperUnMappedField() throws Exception {
        addQueryFieldMappings();
        DocumentParsingException exception = expectThrows(DocumentParsingException.class, () -> {
            mapperService.documentMapper()
                .parse(
                    new SourceToParse(
                        "1",
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder().startObject().field(fieldName, termQuery("unmapped_field", "value")).endObject()
                        ),
                        XContentType.JSON
                    )
                );
        });
        assertThat(exception.getCause(), instanceOf(QueryShardException.class));
        assertThat(exception.getCause().getMessage(), equalTo("No field mapping can be found for the field with name [unmapped_field]"));
    }

    public void testPercolatorFieldMapper_noQuery() throws Exception {
        addQueryFieldMappings();
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse("1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()), XContentType.JSON)
            );
        assertThat(doc.rootDoc().getFields(fieldType.queryBuilderField.name()), hasSize(0));

        try {
            mapperService.documentMapper()
                .parse(
                    new SourceToParse(
                        "1",
                        BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField(fieldName).endObject()),
                        XContentType.JSON
                    )
                );
        } catch (DocumentParsingException e) {
            assertThat(e.getMessage(), containsString("query malformed, must start with start_object"));
        }
    }

    public void testAllowNoAdditionalSettings() throws Exception {
        addQueryFieldMappings();
        IndexService indexServiceWithoutSettings = createIndex("test1", Settings.EMPTY);

        String percolatorMapper = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("doc")
                .startObject("properties")
                .startObject(fieldName)
                .field("type", "percolator")
                .field("index", "no")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> indexServiceWithoutSettings.mapperService()
                .merge("doc", new CompressedXContent(percolatorMapper), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("Mapping definition for [" + fieldName + "] has unsupported parameters:  [index : no]"));
    }

    // multiple percolator fields are allowed in the mapping, but only one field can be used at index time.
    public void testMultiplePercolatorFields() throws Exception {
        String typeName = "doc";
        String percolatorMapper = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(typeName)
                .startObject("properties")
                .startObject("query_field1")
                .field("type", "percolator")
                .endObject()
                .startObject("query_field2")
                .field("type", "percolator")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MergeReason.MAPPING_UPDATE);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject().field("query_field1", queryBuilder).field("query_field2", queryBuilder).endObject()
                    ),
                    XContentType.JSON
                )
            );
        assertThat(doc.rootDoc().getFields().size(), equalTo(15)); // also includes all other meta fields
        BytesRef queryBuilderAsBytes = doc.rootDoc().getField("query_field1.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        queryBuilderAsBytes = doc.rootDoc().getField("query_field2.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);
    }

    // percolator field can be nested under an object field, but only one query can be specified per document
    public void testNestedPercolatorField() throws Exception {
        String typeName = "doc";
        String percolatorMapper = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(typeName)
                .startObject("properties")
                .startObject("object_field")
                .field("type", "object")
                .startObject("properties")
                .startObject("query_field")
                .field("type", "percolator")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MergeReason.MAPPING_UPDATE);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject().startObject("object_field").field("query_field", queryBuilder).endObject().endObject()
                    ),
                    XContentType.JSON
                )
            );
        assertThat(doc.rootDoc().getFields().size(), equalTo(10)); // also includes all other meta fields
        IndexableField queryBuilderField = doc.rootDoc().getField("object_field.query_field.query_builder_field");
        assertTrue(queryBuilderField.fieldType().omitNorms());
        IndexableField extractionResultField = doc.rootDoc().getField("object_field.query_field.extraction_result");
        assertTrue(extractionResultField.fieldType().omitNorms());
        BytesRef queryBuilderAsBytes = queryBuilderField.binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startArray("object_field")
                            .startObject()
                            .field("query_field", queryBuilder)
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    XContentType.JSON
                )
            );
        assertThat(doc.rootDoc().getFields().size(), equalTo(10)); // also includes all other meta fields
        queryBuilderAsBytes = doc.rootDoc().getField("object_field.query_field.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> {
            mapperService.documentMapper()
                .parse(
                    new SourceToParse(
                        "1",
                        BytesReference.bytes(
                            jsonBuilder().startObject()
                                .startArray("object_field")
                                .startObject()
                                .field("query_field", queryBuilder)
                                .endObject()
                                .startObject()
                                .field("query_field", queryBuilder)
                                .endObject()
                                .endArray()
                                .endObject()
                        ),
                        XContentType.JSON
                    )
                );
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("a document can only contain one percolator query"));
    }

    public void testUnsupportedQueries() throws IOException {
        RangeQueryBuilder rangeQuery1 = new RangeQueryBuilder("field").from("2016-01-01||/D").to("2017-01-01||/D");
        assertNotNull(PercolatorFieldMapper.parseQueryBuilder(createParserContext(new ConstantScoreQueryBuilder(rangeQuery1))));

        RangeQueryBuilder rangeQuery2 = new RangeQueryBuilder("field").from("2016-01-01||/D").to("now");
        assertNotNull(PercolatorFieldMapper.parseQueryBuilder(createParserContext(rangeQuery2)));

        HasChildQueryBuilder hasChildQuery = new HasChildQueryBuilder("parent", new MatchAllQueryBuilder(), ScoreMode.None);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(hasChildQuery)));

        BoolQueryBuilder boolQuery1 = new BoolQueryBuilder().must(hasChildQuery);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(boolQuery1)));

        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder().add(hasChildQuery);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(disMaxQuery)));

        ConstantScoreQueryBuilder constantScoreQuery = new ConstantScoreQueryBuilder(hasChildQuery);
        expectThrows(
            IllegalArgumentException.class,
            () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(constantScoreQuery))
        );

        BoostingQueryBuilder boostingQuery1 = new BoostingQueryBuilder(rangeQuery1, new MatchAllQueryBuilder()).negativeBoost(1f);
        assertNotNull(PercolatorFieldMapper.parseQueryBuilder(createParserContext(boostingQuery1)));

        BoostingQueryBuilder boostingQuery2 = new BoostingQueryBuilder(hasChildQuery, new MatchAllQueryBuilder()).negativeBoost(1f);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(boostingQuery2)));

        FunctionScoreQueryBuilder functionScoreQuery = new FunctionScoreQueryBuilder(rangeQuery1, new RandomScoreFunctionBuilder());
        assertNotNull(PercolatorFieldMapper.parseQueryBuilder(createParserContext(functionScoreQuery)));

        FunctionScoreQueryBuilder functionScoreQuery2 = new FunctionScoreQueryBuilder(hasChildQuery, new RandomScoreFunctionBuilder());
        expectThrows(
            IllegalArgumentException.class,
            () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(functionScoreQuery2))
        );

        BoolQueryBuilder boolQuery2 = new BoolQueryBuilder().must(hasChildQuery);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(boolQuery2)));

        HasParentQueryBuilder hasParentQuery = new HasParentQueryBuilder("parent", new MatchAllQueryBuilder(), false);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(hasParentQuery)));

        BoolQueryBuilder boolQuery3 = new BoolQueryBuilder().must(hasParentQuery);
        expectThrows(IllegalArgumentException.class, () -> PercolatorFieldMapper.parseQueryBuilder(createParserContext(boolQuery3)));
    }

    private DocumentParserContext createParserContext(QueryBuilder queryBuilder) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(queryBuilder));
        return new TestDocumentParserContext(parser);
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
                TransportVersion.readVersion(input);
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                assertThat(queryBuilder, equalTo(expected));
            }
        }
    }

    public void testEmptyName() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("")
                .field("type", "percolator")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.parseMapping("type1", MergeReason.MAPPING_UPDATE, new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("field name cannot be an empty string"));
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

        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .rawField(fieldName, new BytesArray(Strings.toString(query)).streamInput(), query.contentType())
                            .endObject()
                    ),
                    XContentType.JSON
                )
            );
        BytesRef querySource = doc.rootDoc().getFields(fieldType.queryBuilderField.name()).get(0).binaryValue();
        try (InputStream in = new ByteArrayInputStream(querySource.bytes, querySource.offset, querySource.length)) {
            try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), writableRegistry())) {
                // Query builder's content is stored via BinaryFieldMapper, which has a custom encoding
                // to encode multiple binary values into a single binary doc values field.
                // This is the reason we need to first need to read the number of values and
                // then the length of the field value in bytes.
                input.readVInt();
                input.readVInt();
                TransportVersion.readVersion(input);
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

        doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .rawField(fieldName, new BytesArray(Strings.toString(query)).streamInput(), query.contentType())
                            .endObject()
                    ),
                    XContentType.JSON
                )
            );
        querySource = doc.rootDoc().getFields(fieldType.queryBuilderField.name()).get(0).binaryValue();
        try (InputStream in = new ByteArrayInputStream(querySource.bytes, querySource.offset, querySource.length)) {
            try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), writableRegistry())) {
                input.readVInt();
                input.readVInt();
                TransportVersion.readVersion(input);
                FunctionScoreQueryBuilder queryBuilder = (FunctionScoreQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
                ScriptScoreFunctionBuilder function = (ScriptScoreFunctionBuilder) queryBuilder.filterFunctionBuilders()[0]
                    .getScoreFunction();
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
                case 0 -> {
                    encodedFrom = new byte[Integer.BYTES];
                    IntPoint.encodeDimension(randomFrom, encodedFrom, 0);
                }
                case 1 -> {
                    encodedFrom = new byte[Long.BYTES];
                    LongPoint.encodeDimension(randomFrom, encodedFrom, 0);
                }
                default -> throw new AssertionError("unexpected encoding type [" + encodingType + "]");
            }

            final int randomTo = randomIntBetween(randomFrom, Integer.MAX_VALUE);
            final byte[] encodedTo;
            switch (encodingType) {
                case 0 -> {
                    encodedTo = new byte[Integer.BYTES];
                    IntPoint.encodeDimension(randomTo, encodedTo, 0);
                }
                case 1 -> {
                    encodedTo = new byte[Long.BYTES];
                    LongPoint.encodeDimension(randomTo, encodedTo, 0);
                }
                default -> throw new AssertionError("unexpected encoding type [" + encodingType + "]");
            }

            String randomFieldName = randomAlphaOfLength(5);
            byte[] result = PercolatorFieldMapper.encodeRange(randomFieldName, encodedFrom, encodedTo);
            assertEquals(32, result.length);

            BytesRef fieldAsBytesRef = new BytesRef(randomFieldName);
            MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
            MurmurHash3.hash128(fieldAsBytesRef.bytes, fieldAsBytesRef.offset, fieldAsBytesRef.length, 0, hash);

            switch (encodingType) {
                case 0 -> {
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 0, 8)).getLong());
                    assertEquals(randomFrom, IntPoint.decodeDimension(subByteArray(result, 12, 4), 0));
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 16, 8)).getLong());
                    assertEquals(randomTo, IntPoint.decodeDimension(subByteArray(result, 28, 4), 0));
                }
                case 1 -> {
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 0, 8)).getLong());
                    assertEquals(randomFrom, LongPoint.decodeDimension(subByteArray(result, 8, 8), 0));
                    assertEquals(hash.h1, ByteBuffer.wrap(subByteArray(result, 16, 8)).getLong());
                    assertEquals(randomTo, LongPoint.decodeDimension(subByteArray(result, 24, 8), 0));
                }
                default -> throw new AssertionError("unexpected encoding type [" + encodingType + "]");
            }
        }
    }

    public void testDuplicatedClauses() throws Exception {
        addQueryFieldMappings();

        QueryBuilder qb = boolQuery().must(boolQuery().must(termQuery("field", "value1")).must(termQuery("field", "value2")))
            .must(boolQuery().must(termQuery("field", "value2")).must(termQuery("field", "value3")));
        ParsedDocument doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, qb).endObject()),
                    XContentType.JSON
                )
            );

        List<String> values = (doc.rootDoc().getFields(fieldType.queryTermsField.name()).stream()).map(f -> f.binaryValue().utf8ToString())
            .sorted()
            .collect(Collectors.toList());
        assertThat(values.size(), equalTo(3));
        assertThat(values.get(0), equalTo("field\0value1"));
        assertThat(values.get(1), equalTo("field\0value2"));
        assertThat(values.get(2), equalTo("field\0value3"));
        int msm = doc.rootDoc().getFields(fieldType.minimumShouldMatchField.name()).get(0).numericValue().intValue();
        assertThat(msm, equalTo(3));

        qb = boolQuery().must(boolQuery().must(termQuery("field", "value1")).must(termQuery("field", "value2")))
            .must(boolQuery().must(termQuery("field", "value2")).must(termQuery("field", "value3")))
            .must(boolQuery().must(termQuery("field", "value3")).must(termQuery("field", "value4")))
            .must(boolQuery().should(termQuery("field", "value4")).should(termQuery("field", "value5")));
        doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, qb).endObject()),
                    XContentType.JSON
                )
            );

        values = doc.rootDoc()
            .getFields(fieldType.queryTermsField.name())
            .stream()
            .map(f -> f.binaryValue().utf8ToString())
            .sorted()
            .collect(Collectors.toList());
        assertThat(values.size(), equalTo(5));
        assertThat(values.get(0), equalTo("field\0value1"));
        assertThat(values.get(1), equalTo("field\0value2"));
        assertThat(values.get(2), equalTo("field\0value3"));
        assertThat(values.get(3), equalTo("field\0value4"));
        assertThat(values.get(4), equalTo("field\0value5"));
        msm = doc.rootDoc().getFields(fieldType.minimumShouldMatchField.name()).get(0).numericValue().intValue();
        assertThat(msm, equalTo(4));

        qb = boolQuery().minimumShouldMatch(3)
            .should(boolQuery().should(termQuery("field", "value1")).should(termQuery("field", "value2")))
            .should(boolQuery().should(termQuery("field", "value2")).should(termQuery("field", "value3")))
            .should(boolQuery().should(termQuery("field", "value3")).should(termQuery("field", "value4")))
            .should(boolQuery().should(termQuery("field", "value4")).should(termQuery("field", "value5")));
        doc = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(fieldName, qb).endObject()),
                    XContentType.JSON
                )
            );

        values = doc.rootDoc()
            .getFields(fieldType.queryTermsField.name())
            .stream()
            .map(f -> f.binaryValue().utf8ToString())
            .sorted()
            .collect(Collectors.toList());
        assertThat(values.size(), equalTo(5));
        assertThat(values.get(0), equalTo("field\0value1"));
        assertThat(values.get(1), equalTo("field\0value2"));
        assertThat(values.get(2), equalTo("field\0value3"));
        assertThat(values.get(3), equalTo("field\0value4"));
        assertThat(values.get(4), equalTo("field\0value5"));
        msm = doc.rootDoc().getFields(fieldType.minimumShouldMatchField.name()).get(0).numericValue().intValue();
        assertThat(msm, equalTo(1));
    }

    private static byte[] subByteArray(byte[] source, int offset, int length) {
        return Arrays.copyOfRange(source, offset, offset + length);
    }

    // Just so that we store scripts in percolator queries, but not really execute these scripts.
    public static class FoolMeScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of("return true", (vars) -> true, "score", (vars) -> 0f);
        }

        @Override
        public String pluginScriptLang() {
            return Script.DEFAULT_SCRIPT_LANG;
        }
    }

    public static class CustomQueriesPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return Collections.singletonList(
                new QuerySpec<QueryBuilder>(
                    CustomParserQueryBuilder.NAME,
                    CustomParserQueryBuilder::new,
                    CustomParserQueryBuilder::fromXContent
                )
            );
        }
    }

    public static final class CustomParserQueryBuilder extends AbstractQueryBuilder<CustomParserQueryBuilder> {
        private static final String NAME = "CUSTOM";

        CustomParserQueryBuilder() {}

        CustomParserQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
            // only the superclass has state
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            return new DummyQueryParserPlugin.DummyQuery();
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        protected boolean doEquals(CustomParserQueryBuilder other) {
            return true;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.array("list", "value0", "value1", "value2");
            builder.array("listOrdered", "value0", "value1", "value2");
            builder.field("map");
            builder.map(Map.of("key1", "value1", "key2", "value2"));
            builder.field("mapOrdered");
            builder.map(Map.of("key3", "value3", "key4", "value4"));
            builder.field("mapStrings");
            builder.map(Map.of("key5", "value5", "key6", "value6"));
            builder.field("mapSupplier");
            builder.map(Map.of("key7", "value7", "key8", "value8"));
            builder.endObject();
        }

        public static CustomParserQueryBuilder fromXContent(XContentParser parser) throws IOException {
            {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("list", parser.currentName());
                List<Object> list = parser.list();
                assertEquals(3, list.size());
                for (int i = 0; i < 3; i++) {
                    assertEquals("value" + i, list.get(i).toString());
                }
                assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            }
            {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("listOrdered", parser.currentName());
                List<Object> listOrdered = parser.listOrderedMap();
                assertEquals(3, listOrdered.size());
                for (int i = 0; i < 3; i++) {
                    assertEquals("value" + i, listOrdered.get(i).toString());
                }
                assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            }
            {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("map", parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                Map<String, Object> map = parser.map();
                assertEquals(2, map.size());
                assertEquals("value1", map.get("key1").toString());
                assertEquals("value2", map.get("key2").toString());
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            }
            {
                assertEquals("mapOrdered", parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                Map<String, Object> mapOrdered = parser.mapOrdered();
                assertEquals(2, mapOrdered.size());
                assertEquals("value3", mapOrdered.get("key3").toString());
                assertEquals("value4", mapOrdered.get("key4").toString());
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            }
            {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("mapStrings", parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                Map<String, Object> mapStrings = parser.map();
                assertEquals(2, mapStrings.size());
                assertEquals("value5", mapStrings.get("key5").toString());
                assertEquals("value6", mapStrings.get("key6").toString());
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            }
            {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("mapSupplier", parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                Map<String, Object> mapSupplier = parser.map(HashMap::new, XContentParser::text);
                assertEquals(2, mapSupplier.size());
                assertEquals("value7", mapSupplier.get("key7").toString());
                assertEquals("value8", mapSupplier.get("key8").toString());
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            }

            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return new CustomParserQueryBuilder();
        }
    }
}
