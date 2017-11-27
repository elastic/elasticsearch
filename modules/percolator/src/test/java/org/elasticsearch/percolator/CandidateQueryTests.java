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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.CoveringQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.common.network.InetAddresses.forString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CandidateQueryTests extends ESSingleNodeTestCase {

    private Directory directory;
    private IndexWriter indexWriter;
    private DocumentMapper documentMapper;
    private DirectoryReader directoryReader;
    private MapperService mapperService;

    private PercolatorFieldMapper fieldMapper;
    private PercolatorFieldMapper.FieldType fieldType;

    private List<Query> queries;
    private PercolateQuery.QueryStore queryStore;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Before
    public void init() throws Exception {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        indexWriter = new IndexWriter(directory, config);

        String indexName = "test";
        IndexService indexService = createIndex(indexName, Settings.EMPTY);
        mapperService = indexService.mapperService();

        String mapper = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int_field").field("type", "integer").endObject()
                .startObject("long_field").field("type", "long").endObject()
                .startObject("half_float_field").field("type", "half_float").endObject()
                .startObject("float_field").field("type", "float").endObject()
                .startObject("double_field").field("type", "double").endObject()
                .startObject("ip_field").field("type", "ip").endObject()
                .startObject("field").field("type", "keyword").endObject()
                .endObject().endObject().endObject().string();
        documentMapper = mapperService.merge("type", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE, true);

        String queryField = "query_field";
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject(queryField).field("type", "percolator").endObject().endObject()
                .endObject().endObject().string();
        mapperService.merge("type", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);
        fieldMapper = (PercolatorFieldMapper) mapperService.documentMapper("type").mappers().getMapper(queryField);
        fieldType = (PercolatorFieldMapper.FieldType) fieldMapper.fieldType();

        queries = new ArrayList<>();
        queryStore = ctx -> docId -> this.queries.get(docId);
    }

    @After
    public void deinit() throws Exception {
        directoryReader.close();
        directory.close();
    }

    public void testDuel() throws Exception {
        List<Function<String, Query>> queryFunctions = new ArrayList<>();
        queryFunctions.add((id) -> new PrefixQuery(new Term("field", id)));
        queryFunctions.add((id) -> new WildcardQuery(new Term("field", id + "*")));
        queryFunctions.add((id) -> new CustomQuery(new Term("field", id)));
        queryFunctions.add((id) -> new SpanTermQuery(new Term("field", id)));
        queryFunctions.add((id) -> new TermQuery(new Term("field", id)));
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("field", id)), BooleanClause.Occur.MUST);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), BooleanClause.Occur.MUST_NOT);
            }
            if (randomBoolean()) {
                builder.add(new CustomQuery(new Term("field", id)), BooleanClause.Occur.MUST);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("field", id)), BooleanClause.Occur.SHOULD);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), BooleanClause.Occur.MUST_NOT);
            }
            if (randomBoolean()) {
                builder.add(new CustomQuery(new Term("field", id)), BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), BooleanClause.Occur.MUST_NOT);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), BooleanClause.Occur.MUST_NOT);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(randomIntBetween(0, 4));
            builder.add(new TermQuery(new Term("field", id)), BooleanClause.Occur.SHOULD);
            builder.add(new CustomQuery(new Term("field", id)), BooleanClause.Occur.SHOULD);
            return builder.build();
        });
        queryFunctions.add((id) -> new MatchAllDocsQuery());
        queryFunctions.add((id) -> new MatchNoDocsQuery("no reason at all"));

        int numDocs = randomIntBetween(queryFunctions.size(), queryFunctions.size() * 3);
        List<ParseContext.Document> documents = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            Query query = queryFunctions.get(i % queryFunctions.size()).apply(id);
            addQuery(query, documents);
        }

        indexWriter.addDocuments(documents);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        // Disable query cache, because ControlQuery cannot be cached...
        shardSearcher.setQueryCache(null);

        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            Iterable<? extends IndexableField> doc = Collections.singleton(new StringField("field", id, Field.Store.NO));
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
            duelRun(queryStore, memoryIndex, shardSearcher);
        }

        Iterable<? extends IndexableField> doc = Collections.singleton(new StringField("field", "value", Field.Store.NO));
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);
        // Empty percolator doc:
        memoryIndex = new MemoryIndex();
        duelRun(queryStore, memoryIndex, shardSearcher);
    }

    public void testDuelSpecificQueries() throws Exception {
        List<ParseContext.Document> documents = new ArrayList<>();

        CommonTermsQuery commonTermsQuery = new CommonTermsQuery(BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, 128);
        commonTermsQuery.add(new Term("field", "quick"));
        commonTermsQuery.add(new Term("field", "brown"));
        commonTermsQuery.add(new Term("field", "fox"));
        addQuery(commonTermsQuery, documents);

        BlendedTermQuery blendedTermQuery = BlendedTermQuery.dismaxBlendedQuery(new Term[]{new Term("field", "quick"),
                new Term("field", "brown"), new Term("field", "fox")}, 1.0f);
        addQuery(blendedTermQuery, documents);

        SpanNearQuery spanNearQuery = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "quick")))
                .addClause(new SpanTermQuery(new Term("field", "brown")))
                .addClause(new SpanTermQuery(new Term("field", "fox")))
                .build();
        addQuery(spanNearQuery, documents);

        SpanNearQuery spanNearQuery2 = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "the")))
                .addClause(new SpanTermQuery(new Term("field", "lazy")))
                .addClause(new SpanTermQuery(new Term("field", "doc")))
                .build();
        SpanOrQuery spanOrQuery = new SpanOrQuery(
                spanNearQuery,
                spanNearQuery2
        );
        addQuery(spanOrQuery, documents);

        SpanNotQuery spanNotQuery = new SpanNotQuery(spanNearQuery, spanNearQuery);
        addQuery(spanNotQuery, documents);

        long lowerLong = randomIntBetween(0, 256);
        long upperLong = lowerLong + randomIntBetween(0, 32);
        addQuery(LongPoint.newRangeQuery("long_field", lowerLong, upperLong), documents);

        indexWriter.addDocuments(documents);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        // Disable query cache, because ControlQuery cannot be cached...
        shardSearcher.setQueryCache(null);

        Document document = new Document();
        document.add(new TextField("field", "the quick brown fox jumps over the lazy dog", Field.Store.NO));
        long randomLong = randomIntBetween((int) lowerLong, (int) upperLong);
        document.add(new LongPoint("long_field", randomLong));
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(document, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);
    }

    public void testRangeQueries() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();
        addQuery(IntPoint.newRangeQuery("int_field", 0, 5), docs);
        addQuery(LongPoint.newRangeQuery("long_field", 5L, 10L), docs);
        addQuery(HalfFloatPoint.newRangeQuery("half_float_field", 10, 15), docs);
        addQuery(FloatPoint.newRangeQuery("float_field", 15, 20), docs);
        addQuery(DoublePoint.newRangeQuery("double_field", 20, 25), docs);
        addQuery(InetAddressPoint.newRangeQuery("ip_field", forString("192.168.0.1"), forString("192.168.0.10")), docs);
        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = Version.V_6_1_0;
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new IntPoint("int_field", 3)), new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
        TopDocs topDocs = shardSearcher.search(query, 1);
        assertEquals(1L, topDocs.totalHits);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new LongPoint("long_field", 7L)), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1L, topDocs.totalHits);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(1, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new HalfFloatPoint("half_float_field", 12)),
            new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1L, topDocs.totalHits);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(2, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new FloatPoint("float_field", 17)), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1, topDocs.totalHits);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(3, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new DoublePoint("double_field", 21)), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1, topDocs.totalHits);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(4, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new InetAddressPoint("ip_field",
            forString("192.168.0.4"))), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1, topDocs.totalHits);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(5, topDocs.scoreDocs[0].doc);
    }

    public void testDuelRangeQueries() throws Exception {
        List<ParseContext.Document> documents = new ArrayList<>();

        int lowerInt = randomIntBetween(0, 256);
        int upperInt = lowerInt + randomIntBetween(0, 32);
        addQuery(IntPoint.newRangeQuery("int_field", lowerInt, upperInt), documents);

        long lowerLong = randomIntBetween(0, 256);
        long upperLong = lowerLong + randomIntBetween(0, 32);
        addQuery(LongPoint.newRangeQuery("long_field", lowerLong, upperLong), documents);

        float lowerHalfFloat = randomIntBetween(0, 256);
        float upperHalfFloat = lowerHalfFloat + randomIntBetween(0, 32);
        addQuery(HalfFloatPoint.newRangeQuery("half_float_field", lowerHalfFloat, upperHalfFloat), documents);

        float lowerFloat = randomIntBetween(0, 256);
        float upperFloat = lowerFloat + randomIntBetween(0, 32);
        addQuery(FloatPoint.newRangeQuery("float_field", lowerFloat, upperFloat), documents);

        double lowerDouble = randomDoubleBetween(0, 256, true);
        double upperDouble = lowerDouble + randomDoubleBetween(0, 32, true);
        addQuery(DoublePoint.newRangeQuery("double_field", lowerDouble, upperDouble), documents);

        int lowerIpPart = randomIntBetween(0, 255);
        int upperIpPart = randomIntBetween(lowerIpPart, 255);
        addQuery(InetAddressPoint.newRangeQuery("ip_field", forString("192.168.1." + lowerIpPart),
            forString("192.168.1." + upperIpPart)), documents);

        indexWriter.addDocuments(documents);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        // Disable query cache, because ControlQuery cannot be cached...
        shardSearcher.setQueryCache(null);

        int randomInt = randomIntBetween(lowerInt, upperInt);
        Iterable<? extends IndexableField> doc = Collections.singleton(new IntPoint("int_field", randomInt));
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        TopDocs result = executeQuery(queryStore, memoryIndex, shardSearcher);
        assertThat(result.scoreDocs.length, equalTo(1));
        assertThat(result.scoreDocs[0].doc, equalTo(0));
        duelRun(queryStore, memoryIndex, shardSearcher);
        doc = Collections.singleton(new IntPoint("int_field", randomInt()));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);

        long randomLong = randomIntBetween((int) lowerLong, (int) upperLong);
        doc = Collections.singleton(new LongPoint("long_field", randomLong));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        result = executeQuery(queryStore, memoryIndex, shardSearcher);
        assertThat(result.scoreDocs.length, equalTo(1));
        assertThat(result.scoreDocs[0].doc, equalTo(1));
        duelRun(queryStore, memoryIndex, shardSearcher);
        doc = Collections.singleton(new LongPoint("long_field", randomLong()));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);

        float randomHalfFloat = randomIntBetween((int) lowerHalfFloat, (int) upperHalfFloat);
        doc = Collections.singleton(new HalfFloatPoint("half_float_field", randomHalfFloat));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        result = executeQuery(queryStore, memoryIndex, shardSearcher);
        assertThat(result.scoreDocs.length, equalTo(1));
        assertThat(result.scoreDocs[0].doc, equalTo(2));
        duelRun(queryStore, memoryIndex, shardSearcher);
        doc = Collections.singleton(new HalfFloatPoint("half_float_field", randomFloat()));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);

        float randomFloat = randomIntBetween((int) lowerFloat, (int) upperFloat);
        doc = Collections.singleton(new FloatPoint("float_field", randomFloat));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        result = executeQuery(queryStore, memoryIndex, shardSearcher);
        assertThat(result.scoreDocs.length, equalTo(1));
        assertThat(result.scoreDocs[0].doc, equalTo(3));
        duelRun(queryStore, memoryIndex, shardSearcher);
        doc = Collections.singleton(new FloatPoint("float_field", randomFloat()));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);

        double randomDouble = randomDoubleBetween(lowerDouble, upperDouble, true);
        doc = Collections.singleton(new DoublePoint("double_field", randomDouble));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        result = executeQuery(queryStore, memoryIndex, shardSearcher);
        assertThat(result.scoreDocs.length, equalTo(1));
        assertThat(result.scoreDocs[0].doc, equalTo(4));
        duelRun(queryStore, memoryIndex, shardSearcher);
        doc = Collections.singleton(new DoublePoint("double_field", randomFloat()));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);

        doc = Collections.singleton(new InetAddressPoint("ip_field",
            forString("192.168.1." + randomIntBetween(lowerIpPart, upperIpPart))));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        result = executeQuery(queryStore, memoryIndex, shardSearcher);
        assertThat(result.scoreDocs.length, equalTo(1));
        assertThat(result.scoreDocs[0].doc, equalTo(5));
        duelRun(queryStore, memoryIndex, shardSearcher);
        doc = Collections.singleton(new InetAddressPoint("ip_field",
            forString("192.168.1." + randomIntBetween(0, 255))));
        memoryIndex = MemoryIndex.fromDocument(doc, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);
    }

    public void testPercolateSmallAndLargeDocument() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.MUST);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("field", "value3")), BooleanClause.Occur.MUST);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value3")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("field", "value4")), BooleanClause.Occur.MUST);
        addQuery(builder.build(), docs);
        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = Version.CURRENT;

        try (RAMDirectory directory = new RAMDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig())) {
                Document document = new Document();
                document.add(new StringField("field", "value1", Field.Store.NO));
                document.add(new StringField("field", "value2", Field.Store.NO));
                iw.addDocument(document);
                document = new Document();
                document.add(new StringField("field", "value5", Field.Store.NO));
                document.add(new StringField("field", "value6", Field.Store.NO));
                iw.addDocument(document);
                document = new Document();
                document.add(new StringField("field", "value3", Field.Store.NO));
                document.add(new StringField("field", "value4", Field.Store.NO));
                iw.addDocument(document);
            }
            try (IndexReader ir = DirectoryReader.open(directory)){
                IndexSearcher percolateSearcher = new IndexSearcher(ir);
                PercolateQuery query = (PercolateQuery)
                    fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
                BooleanQuery candidateQuery = (BooleanQuery) query.getCandidateMatchesQuery();
                assertThat(candidateQuery.clauses().get(0).getQuery(), instanceOf(CoveringQuery.class));
                TopDocs topDocs = shardSearcher.search(query, 10);
                assertEquals(2L, topDocs.totalHits);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(0, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);

                topDocs = shardSearcher.search(new ConstantScoreQuery(query), 10);
                assertEquals(2L, topDocs.totalHits);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(0, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);
            }
        }

        // This will trigger using the TermsQuery instead of individual term query clauses in the CoveringQuery:
        try (RAMDirectory directory = new RAMDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig())) {
                Document document = new Document();
                for (int i = 0; i < 1024; i++) {
                    int fieldNumber = 2 + i;
                    document.add(new StringField("field", "value" + fieldNumber, Field.Store.NO));
                }
                iw.addDocument(document);
            }
            try (IndexReader ir = DirectoryReader.open(directory)){
                IndexSearcher percolateSearcher = new IndexSearcher(ir);
                PercolateQuery query = (PercolateQuery)
                    fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, v);
                BooleanQuery candidateQuery = (BooleanQuery) query.getCandidateMatchesQuery();
                assertThat(candidateQuery.clauses().get(0).getQuery(), instanceOf(TermInSetQuery.class));

                TopDocs topDocs = shardSearcher.search(query, 10);
                assertEquals(2L, topDocs.totalHits);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(1, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);

                topDocs = shardSearcher.search(new ConstantScoreQuery(query), 10);
                assertEquals(2L, topDocs.totalHits);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(1, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);
            }
        }
    }

    private void duelRun(PercolateQuery.QueryStore queryStore, MemoryIndex memoryIndex, IndexSearcher shardSearcher) throws IOException {
        boolean requireScore = randomBoolean();
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query percolateQuery = fieldType.percolateQuery("_name", queryStore,
            Collections.singletonList(new BytesArray("{}")), percolateSearcher, Version.CURRENT);
        Query query = requireScore ? percolateQuery : new ConstantScoreQuery(percolateQuery);
        TopDocs topDocs = shardSearcher.search(query, 10);

        Query controlQuery = new ControlQuery(memoryIndex, queryStore);
        controlQuery = requireScore ? controlQuery : new ConstantScoreQuery(controlQuery);
        TopDocs controlTopDocs = shardSearcher.search(controlQuery, 10);
        assertThat(topDocs.totalHits, equalTo(controlTopDocs.totalHits));
        assertThat(topDocs.scoreDocs.length, equalTo(controlTopDocs.scoreDocs.length));
        for (int j = 0; j < topDocs.scoreDocs.length; j++) {
            assertThat(topDocs.scoreDocs[j].doc, equalTo(controlTopDocs.scoreDocs[j].doc));
            assertThat(topDocs.scoreDocs[j].score, equalTo(controlTopDocs.scoreDocs[j].score));
            if (requireScore) {
                Explanation explain1 = shardSearcher.explain(query, topDocs.scoreDocs[j].doc);
                Explanation explain2 = shardSearcher.explain(controlQuery, controlTopDocs.scoreDocs[j].doc);
                assertThat(explain1.isMatch(), equalTo(explain2.isMatch()));
                assertThat(explain1.getValue(), equalTo(explain2.getValue()));
            }
        }
    }

    private void addQuery(Query query, List<ParseContext.Document> docs) throws IOException {
        ParseContext.InternalParseContext parseContext = new ParseContext.InternalParseContext(Settings.EMPTY,
                mapperService.documentMapperParser(), documentMapper, null, null);
        fieldMapper.processQuery(query, parseContext);
        docs.add(parseContext.doc());
        queries.add(query);
    }

    private TopDocs executeQuery(PercolateQuery.QueryStore queryStore,
                                 MemoryIndex memoryIndex,
                                 IndexSearcher shardSearcher) throws IOException {
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query percolateQuery = fieldType.percolateQuery("_name", queryStore,
            Collections.singletonList(new BytesArray("{}")), percolateSearcher, Version.CURRENT);
        return shardSearcher.search(percolateQuery, 10);
    }

    private static final class CustomQuery extends Query {

        private final Term term;

        private CustomQuery(Term term) {
            this.term = term;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            return new TermQuery(term);
        }

        @Override
        public String toString(String field) {
            return "custom{" + field + "}";
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj);
        }

        @Override
        public int hashCode() {
            return classHash();
        }
    }

    private static final class ControlQuery extends Query {

        private final MemoryIndex memoryIndex;
        private final PercolateQuery.QueryStore queryStore;

        private ControlQuery(MemoryIndex memoryIndex, PercolateQuery.QueryStore queryStore) {
            this.memoryIndex = memoryIndex;
            this.queryStore = queryStore;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) {
            return new Weight(this) {

                float _score;

                @Override
                public void extractTerms(Set<Term> terms) {}

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    Scorer scorer = scorer(context);
                    if (scorer != null) {
                        int result = scorer.iterator().advance(doc);
                        if (result == doc) {
                            return Explanation.match(scorer.score(), "ControlQuery");
                        }
                    }
                    return Explanation.noMatch("ControlQuery");
                }

                @Override
                public String toString() {
                    return "weight(" + ControlQuery.this + ")";
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    DocIdSetIterator allDocs = DocIdSetIterator.all(context.reader().maxDoc());
                    CheckedFunction<Integer, Query, IOException> leaf = queryStore.getQueries(context);
                    FilteredDocIdSetIterator memoryIndexIterator = new FilteredDocIdSetIterator(allDocs) {

                        @Override
                        protected boolean match(int doc) {
                            try {
                                Query query = leaf.apply(doc);
                                float score = memoryIndex.search(query);
                                if (score != 0f) {
                                    if (needsScores) {
                                        _score = score;
                                    }
                                    return true;
                                } else {
                                    return false;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                    return new FilterScorer(new ConstantScoreScorer(this, 1f, memoryIndexIterator)) {

                        @Override
                        public float score() throws IOException {
                            return _score;
                        }
                    };
                }
            };
        }

        @Override
        public String toString(String field) {
            return "control{" + field + "}";
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj);
        }

        @Override
        public int hashCode() {
            return classHash();
        }

    }

}
