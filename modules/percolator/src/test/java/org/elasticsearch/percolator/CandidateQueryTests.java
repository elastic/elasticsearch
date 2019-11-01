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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.CoveringQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

        String mapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int_field").field("type", "integer").endObject()
                .startObject("long_field").field("type", "long").endObject()
                .startObject("half_float_field").field("type", "half_float").endObject()
                .startObject("float_field").field("type", "float").endObject()
                .startObject("double_field").field("type", "double").endObject()
                .startObject("ip_field").field("type", "ip").endObject()
                .startObject("field").field("type", "keyword").endObject()
                .endObject().endObject().endObject());
        documentMapper = mapperService.merge("type", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE);

        String queryField = "query_field";
        String percolatorMapper = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject(queryField).field("type", "percolator").endObject().endObject()
                .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE);
        fieldMapper = (PercolatorFieldMapper) mapperService.documentMapper().mappers().getMapper(queryField);
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
        int numFields = randomIntBetween(1, 3);
        Map<String, List<String>> stringContent = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            int numTokens = randomIntBetween(1, 64);
            List<String> values = new ArrayList<>();
            for (int j = 0; j < numTokens; j++) {
                values.add(randomAlphaOfLength(8));
            }
            stringContent.put("field" + i, values);
        }
        List<String> stringFields = new ArrayList<>(stringContent.keySet());

        int numValues = randomIntBetween(16, 64);
        List<Integer> intValues = new ArrayList<>(numValues);
        for (int j = 0; j < numValues; j++) {
            intValues.add(randomInt());
        }
        Collections.sort(intValues);

        MappedFieldType intFieldType = mapperService.fullName("int_field");

        List<Supplier<Query>> queryFunctions = new ArrayList<>();
        queryFunctions.add(MatchNoDocsQuery::new);
        queryFunctions.add(MatchAllDocsQuery::new);
        queryFunctions.add(() -> new TermQuery(new Term("unknown_field", "value")));
        String field1 = randomFrom(stringFields);
        queryFunctions.add(() -> new TermQuery(new Term(field1, randomFrom(stringContent.get(field1)))));
        String field2 = randomFrom(stringFields);
        queryFunctions.add(() -> new TermQuery(new Term(field2, randomFrom(stringContent.get(field2)))));
        queryFunctions.add(() -> intFieldType.termQuery(randomFrom(intValues), null));
        queryFunctions.add(() -> intFieldType.termsQuery(Arrays.asList(randomFrom(intValues), randomFrom(intValues)), null));
        queryFunctions.add(() -> intFieldType.rangeQuery(intValues.get(4), intValues.get(intValues.size() - 4), true,
            true, ShapeRelation.WITHIN, null, null, null));
        queryFunctions.add(() -> new TermInSetQuery(field1, new BytesRef(randomFrom(stringContent.get(field1))),
                new BytesRef(randomFrom(stringContent.get(field1)))));
        queryFunctions.add(() -> new TermInSetQuery(field2, new BytesRef(randomFrom(stringContent.get(field1))),
                new BytesRef(randomFrom(stringContent.get(field1)))));
        // many iterations with boolean queries, which are the most complex queries to deal with when nested
        int numRandomBoolQueries = 1000;
        for (int i = 0; i < numRandomBoolQueries; i++) {
            queryFunctions.add(() -> createRandomBooleanQuery(1, stringFields, stringContent, intFieldType, intValues));
        }
        queryFunctions.add(() -> {
            int numClauses = randomIntBetween(1, 1 << randomIntBetween(2, 4));
            List<Query> clauses = new ArrayList<>();
            for (int i = 0; i < numClauses; i++) {
                String field = randomFrom(stringFields);
                clauses.add(new TermQuery(new Term(field, randomFrom(stringContent.get(field)))));
            }
            return new DisjunctionMaxQuery(clauses, 0.01f);
        });
        queryFunctions.add(() -> {
            Float minScore = randomBoolean() ? null : (float) randomIntBetween(1, 1000);
            Query innerQuery;
            if (randomBoolean()) {
                innerQuery = new TermQuery(new Term(field1, randomFrom(stringContent.get(field1))));
            } else {
                innerQuery = new PhraseQuery(field1, randomFrom(stringContent.get(field1)), randomFrom(stringContent.get(field1)));
            }
            return new FunctionScoreQuery(innerQuery, minScore, 1f);
        });

        List<ParseContext.Document> documents = new ArrayList<>();
        for (Supplier<Query> queryFunction : queryFunctions) {
            Query query = queryFunction.get();
            addQuery(query, documents);
        }

        indexWriter.addDocuments(documents);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        // Disable query cache, because ControlQuery cannot be cached...
        shardSearcher.setQueryCache(null);

        Document document = new Document();
        for (Map.Entry<String, List<String>> entry : stringContent.entrySet()) {
            String value = entry.getValue().stream().collect(Collectors.joining(" "));
            document.add(new TextField(entry.getKey(), value, Field.Store.NO));
        }
        for (Integer intValue : intValues) {
            List<Field> numberFields =
                NumberFieldMapper.NumberType.INTEGER.createFields("int_field", intValue, true, true, false);
            for (Field numberField : numberFields) {
                document.add(numberField);
            }
        }
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(document, new WhitespaceAnalyzer());
        duelRun(queryStore, memoryIndex, shardSearcher);
    }

    private BooleanQuery createRandomBooleanQuery(int depth, List<String> fields, Map<String, List<String>> content,
                                                  MappedFieldType intFieldType, List<Integer> intValues) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        int numClauses = randomIntBetween(1, 1 << randomIntBetween(2, 4)); // use low numbers of clauses more often
        int numShouldClauses = 0;
        boolean onlyShouldClauses = rarely();
        for (int i = 0; i < numClauses; i++) {
            Occur occur;
            if (onlyShouldClauses) {
                occur = Occur.SHOULD;
                if (randomBoolean()) {
                    String field = randomFrom(fields);
                    builder.add(new TermQuery(new Term(field, randomFrom(content.get(field)))), occur);
                } else {
                    builder.add(intFieldType.termQuery(randomFrom(intValues), null), occur);
                }
            } else if (rarely() && depth <= 3) {
                occur = randomFrom(Arrays.asList(Occur.FILTER, Occur.MUST, Occur.SHOULD));
                builder.add(createRandomBooleanQuery(depth + 1, fields, content, intFieldType, intValues), occur);
            } else if (rarely()) {
                if (randomBoolean()) {
                    occur = randomFrom(Arrays.asList(Occur.FILTER, Occur.MUST, Occur.SHOULD));
                    if (randomBoolean()) {
                        builder.add(new TermQuery(new Term("unknown_field", randomAlphaOfLength(8))), occur);
                    } else {
                        builder.add(intFieldType.termQuery(randomFrom(intValues), null), occur);
                    }
                } else if (randomBoolean()) {
                    String field = randomFrom(fields);
                    builder.add(new TermQuery(new Term(field, randomFrom(content.get(field)))), occur = Occur.MUST_NOT);
                } else {
                    builder.add(intFieldType.termQuery(randomFrom(intValues), null), occur = Occur.MUST_NOT);
                }
            } else {
                if (randomBoolean()) {
                    occur = randomFrom(Arrays.asList(Occur.FILTER, Occur.MUST, Occur.SHOULD));
                    if (randomBoolean()) {
                        String field = randomFrom(fields);
                        builder.add(new TermQuery(new Term(field, randomFrom(content.get(field)))), occur);
                    } else {
                        builder.add(intFieldType.termQuery(randomFrom(intValues), null), occur);
                    }
                } else {
                    builder.add(new TermQuery(new Term("unknown_field", randomAlphaOfLength(8))), occur = Occur.MUST_NOT);
                }
            }
            if (occur == Occur.SHOULD) {
                numShouldClauses++;
            }
        }
        builder.setMinimumNumberShouldMatch(randomIntBetween(0, numShouldClauses));
        return builder.build();
    }

    public void testDuel2() throws Exception {
        List<String> stringValues = new ArrayList<>();
        stringValues.add("value1");
        stringValues.add("value2");
        stringValues.add("value3");

        MappedFieldType intFieldType = mapperService.fullName("int_field");
        List<int[]> ranges = new ArrayList<>();
        ranges.add(new int[]{-5, 5});
        ranges.add(new int[]{0, 10});
        ranges.add(new int[]{15, 50});

        List<ParseContext.Document> documents = new ArrayList<>();
        {
            addQuery(new TermQuery(new Term("string_field", randomFrom(stringValues))), documents);
        }
        {
            addQuery(new PhraseQuery(0, "string_field", stringValues.toArray(new String[0])), documents);
        }
        {
            int[] range = randomFrom(ranges);
            Query rangeQuery = intFieldType.rangeQuery(range[0], range[1], true, true, null, null, null, null);
            addQuery(rangeQuery, documents);
        }
        {
            int numBooleanQueries = randomIntBetween(1, 5);
            for (int i = 0; i < numBooleanQueries; i++) {
                Query randomBQ = randomBQ(1, stringValues, ranges, intFieldType);
                addQuery(randomBQ, documents);
            }
        }
        {
            addQuery(new MatchNoDocsQuery(), documents);
        }
        {
            addQuery(new MatchAllDocsQuery(), documents);
        }

        indexWriter.addDocuments(documents);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        // Disable query cache, because ControlQuery cannot be cached...
        shardSearcher.setQueryCache(null);

        Document document = new Document();
        for (String value : stringValues) {
            document.add(new TextField("string_field", value, Field.Store.NO));
            logger.info("Test with document: {}" + document);
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(document, new WhitespaceAnalyzer());
            duelRun(queryStore, memoryIndex, shardSearcher);
        }
        for (int[] range : ranges) {
            List<Field> numberFields =
                NumberFieldMapper.NumberType.INTEGER.createFields("int_field", between(range[0], range[1]), true, true, false);
            for (Field numberField : numberFields) {
                document.add(numberField);
            }
            logger.info("Test with document: {}" + document);
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(document, new WhitespaceAnalyzer());
            duelRun(queryStore, memoryIndex, shardSearcher);
        }
    }

    private BooleanQuery randomBQ(int depth, List<String> stringValues, List<int[]> ranges, MappedFieldType intFieldType) {
        final int numClauses = randomIntBetween(1, 4);
        final boolean onlyShouldClauses = randomBoolean();
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        int numShouldClauses = 0;
        for (int i = 0; i < numClauses; i++) {
            Query subQuery;
            if (randomBoolean() && depth <= 3) {
                subQuery = randomBQ(depth + 1, stringValues, ranges, intFieldType);
            } else if (randomBoolean()) {
                int[] range = randomFrom(ranges);
                subQuery = intFieldType.rangeQuery(range[0], range[1], true, true, null, null, null, null);
            } else {
                subQuery = new TermQuery(new Term("string_field", randomFrom(stringValues)));
            }

            Occur occur;
            if (onlyShouldClauses) {
                occur = Occur.SHOULD;
            } else {
                occur = randomFrom(Arrays.asList(Occur.FILTER, Occur.MUST, Occur.SHOULD));
            }
            if (occur == Occur.SHOULD) {
                numShouldClauses++;
            }
            builder.add(subQuery, occur);
        }
        builder.setMinimumNumberShouldMatch(randomIntBetween(0, numShouldClauses));
        return builder.build();
    }

    public void testDuelIdBased() throws Exception {
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
            builder.add(new TermQuery(new Term("field", id)), Occur.MUST);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), Occur.MUST_NOT);
            }
            if (randomBoolean()) {
                builder.add(new CustomQuery(new Term("field", id)), Occur.MUST);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("field", id)), Occur.SHOULD);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), Occur.MUST_NOT);
            }
            if (randomBoolean()) {
                builder.add(new CustomQuery(new Term("field", id)), Occur.SHOULD);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new MatchAllDocsQuery(), Occur.MUST);
            builder.add(new MatchAllDocsQuery(), Occur.MUST);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), Occur.MUST_NOT);
            } else if (randomBoolean()) {
                builder.add(new MatchAllDocsQuery(), Occur.MUST_NOT);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new MatchAllDocsQuery(), Occur.SHOULD);
            builder.add(new MatchAllDocsQuery(), Occur.SHOULD);
            if (randomBoolean()) {
                builder.add(new MatchNoDocsQuery("no reason"), Occur.MUST_NOT);
            } else if (randomBoolean()) {
                builder.add(new MatchAllDocsQuery(), Occur.MUST_NOT);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new MatchAllDocsQuery(), Occur.SHOULD);
            builder.add(new TermQuery(new Term("field", id)), Occur.SHOULD);
            if (randomBoolean()) {
                builder.add(new MatchAllDocsQuery(), Occur.SHOULD);
            }
            if (randomBoolean()) {
                builder.setMinimumNumberShouldMatch(2);
            }
            return builder.build();
        });
        queryFunctions.add((id) -> {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(randomIntBetween(0, 4));
            builder.add(new TermQuery(new Term("field", id)), Occur.SHOULD);
            builder.add(new CustomQuery(new Term("field", id)), Occur.SHOULD);
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

        Version v = VersionUtils.randomIndexCompatibleVersion(random());
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new IntPoint("int_field", 3)), new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")),
                percolateSearcher, false, v);
        TopDocs topDocs = shardSearcher.search(query, 1);
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new LongPoint("long_field", 7L)), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(1, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new HalfFloatPoint("half_float_field", 12)),
            new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(2, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new FloatPoint("float_field", 17)), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(3, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new DoublePoint("double_field", 21)), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(4, topDocs.scoreDocs[0].doc);

        memoryIndex = MemoryIndex.fromDocument(Collections.singleton(new InetAddressPoint("ip_field",
            forString("192.168.0.4"))), new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value);
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

    public void testPercolateMatchAll() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();
        addQuery(new MatchAllDocsQuery(), docs);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value1")), Occur.MUST);
        builder.add(new MatchAllDocsQuery(), Occur.MUST);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        builder.add(new MatchAllDocsQuery(), Occur.MUST);
        builder.add(new MatchAllDocsQuery(), Occur.MUST);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), Occur.MUST);
        builder.add(new MatchAllDocsQuery(), Occur.MUST_NOT);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value2")), Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), Occur.SHOULD);
        addQuery(builder.build(), docs);
        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value1", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        PercolateQuery query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore,
            Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, Version.CURRENT);
        TopDocs topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(3L, topDocs.totalHits.value);
        assertEquals(3, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);
        assertEquals(1, topDocs.scoreDocs[1].doc);
        assertEquals(4, topDocs.scoreDocs[2].doc);

        topDocs = shardSearcher.search(new ConstantScoreQuery(query), 10);
        assertEquals(3L, topDocs.totalHits.value);
        assertEquals(3, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);
        assertEquals(1, topDocs.scoreDocs[1].doc);
        assertEquals(4, topDocs.scoreDocs[2].doc);
    }

    public void testFunctionScoreQuery() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();
        addQuery(new FunctionScoreQuery(new TermQuery(new Term("field", "value")), null, 1f), docs);
        addQuery(new FunctionScoreQuery(new TermQuery(new Term("field", "value")), 10f, 1f), docs);
        addQuery(new FunctionScoreQuery(new MatchAllDocsQuery(), null, 1f), docs);
        addQuery(new FunctionScoreQuery(new MatchAllDocsQuery(), 10F, 1f), docs);

        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        PercolateQuery query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore,
            Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, Version.CURRENT);
        TopDocs topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(2L, topDocs.totalHits.value);
        assertEquals(2, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);
        assertEquals(2, topDocs.scoreDocs[1].doc);
    }

    public void testPercolateSmallAndLargeDocument() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value1")), Occur.MUST);
        builder.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        builder.add(new TermQuery(new Term("field", "value3")), Occur.MUST);
        addQuery(builder.build(), docs);
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value3")), Occur.MUST);
        builder.add(new TermQuery(new Term("field", "value4")), Occur.MUST);
        addQuery(builder.build(), docs);
        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = Version.CURRENT;

        try (RAMDirectory directory = new RAMDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig())) {
                List<Document> documents = new ArrayList<>();
                Document document = new Document();
                document.add(new StringField("field", "value1", Field.Store.NO));
                document.add(new StringField("field", "value2", Field.Store.NO));
                documents.add(document);
                document = new Document();
                document.add(new StringField("field", "value5", Field.Store.NO));
                document.add(new StringField("field", "value6", Field.Store.NO));
                documents.add(document);
                document = new Document();
                document.add(new StringField("field", "value3", Field.Store.NO));
                document.add(new StringField("field", "value4", Field.Store.NO));
                documents.add(document);
                iw.addDocuments(documents); // IW#addDocuments(...) ensures we end up with a single segment
            }
            try (IndexReader ir = DirectoryReader.open(directory)){
                IndexSearcher percolateSearcher = new IndexSearcher(ir);
                PercolateQuery query = (PercolateQuery)
                    fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")),
                            percolateSearcher, false, v);
                BooleanQuery candidateQuery = (BooleanQuery) query.getCandidateMatchesQuery();
                assertThat(candidateQuery.clauses().get(0).getQuery(), instanceOf(CoveringQuery.class));
                TopDocs topDocs = shardSearcher.search(query, 10);
                assertEquals(2L, topDocs.totalHits.value);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(0, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);

                topDocs = shardSearcher.search(new ConstantScoreQuery(query), 10);
                assertEquals(2L, topDocs.totalHits.value);
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
                    fieldType.percolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")),
                            percolateSearcher, false, v);
                BooleanQuery candidateQuery = (BooleanQuery) query.getCandidateMatchesQuery();
                assertThat(candidateQuery.clauses().get(0).getQuery(), instanceOf(TermInSetQuery.class));

                TopDocs topDocs = shardSearcher.search(query, 10);
                assertEquals(2L, topDocs.totalHits.value);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(1, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);

                topDocs = shardSearcher.search(new ConstantScoreQuery(query), 10);
                assertEquals(2L, topDocs.totalHits.value);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals(1, topDocs.scoreDocs[0].doc);
                assertEquals(2, topDocs.scoreDocs[1].doc);
            }
        }
    }

    public void testDuplicatedClauses() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        BooleanQuery.Builder builder1 = new BooleanQuery.Builder();
        builder1.add(new TermQuery(new Term("field", "value1")), Occur.MUST);
        builder1.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        builder.add(builder1.build(), Occur.MUST);
        BooleanQuery.Builder builder2 = new BooleanQuery.Builder();
        builder2.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        builder2.add(new TermQuery(new Term("field", "value3")), Occur.MUST);
        builder.add(builder2.build(), Occur.MUST);
        addQuery(builder.build(), docs);

        builder = new BooleanQuery.Builder()
                .setMinimumNumberShouldMatch(2);
        builder1 = new BooleanQuery.Builder();
        builder1.add(new TermQuery(new Term("field", "value1")), Occur.MUST);
        builder1.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        builder.add(builder1.build(), Occur.SHOULD);
        builder2 = new BooleanQuery.Builder();
        builder2.add(new TermQuery(new Term("field", "value2")), Occur.MUST);
        builder2.add(new TermQuery(new Term("field", "value3")), Occur.MUST);
        builder.add(builder2.build(), Occur.SHOULD);
        BooleanQuery.Builder builder3 = new BooleanQuery.Builder();
        builder3.add(new TermQuery(new Term("field", "value3")), Occur.MUST);
        builder3.add(new TermQuery(new Term("field", "value4")), Occur.MUST);
        builder.add(builder3.build(), Occur.SHOULD);
        addQuery(builder.build(), docs);

        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = Version.CURRENT;
        List<BytesReference> sources = Collections.singletonList(new BytesArray("{}"));

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value1 value2 value3", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        PercolateQuery query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore, sources, percolateSearcher, false, v);
        TopDocs topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(2L, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs[0].doc);
        assertEquals(1, topDocs.scoreDocs[1].doc);
    }

    public void testDuplicatedClauses2() throws Exception {
        List<ParseContext.Document> docs = new ArrayList<>();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(3);
        builder.add(new TermQuery(new Term("field", "value1")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value2")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value2")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value3")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value3")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value3")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value4")), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value5")), Occur.SHOULD);
        addQuery(builder.build(), docs);

        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = Version.CURRENT;
        List<BytesReference> sources = Collections.singletonList(new BytesArray("{}"));

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value1 value4 value5", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        PercolateQuery query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore, sources, percolateSearcher, false, v);
        TopDocs topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs[0].doc);

        memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value1 value2", new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore, sources, percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs[0].doc);

        memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value3", new WhitespaceAnalyzer());
        percolateSearcher = memoryIndex.createSearcher();
        query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore, sources, percolateSearcher, false, v);
        topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs[0].doc);
    }

    public void testMsmAndRanges_disjunction() throws Exception {
        // Recreates a similar scenario that made testDuel() fail randomly:
        // https://github.com/elastic/elasticsearch/issues/29393
        List<ParseContext.Document> docs = new ArrayList<>();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(2);

        BooleanQuery.Builder builder1 = new BooleanQuery.Builder();
        builder1.add(new TermQuery(new Term("field", "value1")), Occur.FILTER);
        builder.add(builder1.build(), Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value2")), Occur.MUST_NOT);
        builder.add(IntPoint.newRangeQuery("int_field", 0, 5), Occur.SHOULD);
        builder.add(IntPoint.newRangeQuery("int_field", 6, 10), Occur.SHOULD);
        addQuery(builder.build(), docs);

        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        shardSearcher.setQueryCache(null);

        Version v = Version.CURRENT;
        List<BytesReference> sources = Collections.singletonList(new BytesArray("{}"));

        Document document = new Document();
        document.add(new IntPoint("int_field", 4));
        document.add(new IntPoint("int_field", 7));
        MemoryIndex memoryIndex = MemoryIndex.fromDocument(document, new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        PercolateQuery query = (PercolateQuery) fieldType.percolateQuery("_name", queryStore, sources, percolateSearcher, false, v);
        TopDocs topDocs = shardSearcher.search(query, 10, new Sort(SortField.FIELD_DOC));
        assertEquals(1L, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs[0].doc);
    }

    private void duelRun(PercolateQuery.QueryStore queryStore, MemoryIndex memoryIndex, IndexSearcher shardSearcher) throws IOException {
        boolean requireScore = randomBoolean();
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query percolateQuery = fieldType.percolateQuery("_name", queryStore,
            Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, Version.CURRENT);
        Query query = requireScore ? percolateQuery : new ConstantScoreQuery(percolateQuery);
        TopDocs topDocs = shardSearcher.search(query, 100);

        Query controlQuery = new ControlQuery(memoryIndex, queryStore);
        controlQuery = requireScore ? controlQuery : new ConstantScoreQuery(controlQuery);
        TopDocs controlTopDocs = shardSearcher.search(controlQuery, 100);

        try {
            assertThat(topDocs.totalHits.value, equalTo(controlTopDocs.totalHits.value));
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
        } catch (AssertionError ae) {
            logger.error("topDocs.totalHits={}", topDocs.totalHits);
            logger.error("controlTopDocs.totalHits={}", controlTopDocs.totalHits);

            logger.error("topDocs.scoreDocs.length={}", topDocs.scoreDocs.length);
            logger.error("controlTopDocs.scoreDocs.length={}", controlTopDocs.scoreDocs.length);

            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                logger.error("topDocs.scoreDocs[{}].doc={}", i, topDocs.scoreDocs[i].doc);
                logger.error("topDocs.scoreDocs[{}].score={}", i, topDocs.scoreDocs[i].score);
            }
            for (int i = 0; i < controlTopDocs.scoreDocs.length; i++) {
                logger.error("controlTopDocs.scoreDocs[{}].doc={}", i, controlTopDocs.scoreDocs[i].doc);
                logger.error("controlTopDocs.scoreDocs[{}].score={}", i, controlTopDocs.scoreDocs[i].score);

                // Additional stored information that is useful when debugging:
                String queryToString = shardSearcher.doc(controlTopDocs.scoreDocs[i].doc).get("query_to_string");
                logger.error("controlTopDocs.scoreDocs[{}].query_to_string={}", i, queryToString);

                TermsEnum tenum = MultiTerms.getTerms(shardSearcher.getIndexReader(), fieldType.queryTermsField.name()).iterator();
                StringBuilder builder = new StringBuilder();
                for (BytesRef term = tenum.next(); term != null; term = tenum.next()) {
                    PostingsEnum penum = tenum.postings(null);
                    if (penum.advance(controlTopDocs.scoreDocs[i].doc) == controlTopDocs.scoreDocs[i].doc) {
                        builder.append(term.utf8ToString()).append(',');
                    }
                }
                logger.error("controlTopDocs.scoreDocs[{}].query_terms_field={}", i, builder.toString());

                NumericDocValues numericValues =
                    MultiDocValues.getNumericValues(shardSearcher.getIndexReader(), fieldType.minimumShouldMatchField.name());
                boolean exact = numericValues.advanceExact(controlTopDocs.scoreDocs[i].doc);
                if (exact) {
                    logger.error("controlTopDocs.scoreDocs[{}].minimum_should_match_field={}", i, numericValues.longValue());
                } else {
                    // Some queries do not have a msm field. (e.g. unsupported queries)
                    logger.error("controlTopDocs.scoreDocs[{}].minimum_should_match_field=[NO_VALUE]", i);
                }
            }
            throw ae;
        }
    }

    private void addQuery(Query query, List<ParseContext.Document> docs) {
        IndexMetaData build = IndexMetaData.builder("")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0).build();
        IndexSettings settings = new IndexSettings(build, Settings.EMPTY);
        ParseContext.InternalParseContext parseContext = new ParseContext.InternalParseContext(settings,
                mapperService.documentMapperParser(), documentMapper, null, null);
        fieldMapper.processQuery(query, parseContext);
        ParseContext.Document queryDocument = parseContext.doc();
        // Add to string representation of the query to make debugging easier:
        queryDocument.add(new StoredField("query_to_string", query.toString()));
        docs.add(queryDocument);
        queries.add(query);
    }

    private TopDocs executeQuery(PercolateQuery.QueryStore queryStore,
                                 MemoryIndex memoryIndex,
                                 IndexSearcher shardSearcher) throws IOException {
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        Query percolateQuery = fieldType.percolateQuery("_name", queryStore,
            Collections.singletonList(new BytesArray("{}")), percolateSearcher, false, Version.CURRENT);
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
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            final IndexSearcher percolatorIndexSearcher = memoryIndex.createSearcher();
            return new Weight(this) {

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
                    float _score[] = new float[]{boost};
                    DocIdSetIterator allDocs = DocIdSetIterator.all(context.reader().maxDoc());
                    CheckedFunction<Integer, Query, IOException> leaf = queryStore.getQueries(context);
                    FilteredDocIdSetIterator memoryIndexIterator = new FilteredDocIdSetIterator(allDocs) {

                        @Override
                        protected boolean match(int doc) {
                            try {
                                Query query = leaf.apply(doc);
                                TopDocs topDocs = percolatorIndexSearcher.search(query, 1);
                                if (topDocs.scoreDocs.length > 0) {
                                    if (scoreMode.needsScores()) {
                                        _score[0] = topDocs.scoreDocs[0].score;
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
                    return new Scorer(this) {

                        @Override
                        public int docID() {
                            return memoryIndexIterator.docID();
                        }

                        @Override
                        public DocIdSetIterator iterator() {
                            return memoryIndexIterator;
                        }

                        @Override
                        public float score() throws IOException {
                            return _score[0];
                        }

                        @Override
                        public float getMaxScore(int upTo) throws IOException {
                            return _score[0];
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false; // doesn't matter
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
