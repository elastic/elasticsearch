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
import org.apache.lucene.document.Field;
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
import org.apache.lucene.search.ConstantScoreWeight;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
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
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

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
        String mappingType = "query";
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(mappingType)
                .startObject("properties").startObject(queryField).field("type", "percolator").endObject().endObject()
                .endObject().endObject().string();
        mapperService.merge(mappingType, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);
        fieldMapper = (PercolatorFieldMapper) mapperService.documentMapper(mappingType).mappers().getMapper(queryField);
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

        BlendedTermQuery blendedTermQuery = BlendedTermQuery.booleanBlendedQuery(new Term[]{new Term("field", "quick"),
                new Term("field", "brown"), new Term("field", "fox")}, false);
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

    private void duelRun(PercolateQuery.QueryStore queryStore, MemoryIndex memoryIndex, IndexSearcher shardSearcher) throws IOException {
        boolean requireScore = randomBoolean();
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query percolateQuery = fieldType.percolateQuery("type", queryStore, new BytesArray("{}"), percolateSearcher);
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
        public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
            return new ConstantScoreWeight(this) {

                float _score;

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
                    return new FilterScorer(new ConstantScoreScorer(this, score(), memoryIndexIterator)) {

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
