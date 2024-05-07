package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LuceneRetrieveOperatorTests extends AnyOperatorTestCase {
    private static final MappedFieldType S_FIELD = new NumberFieldMapper.NumberFieldType("s", NumberFieldMapper.NumberType.LONG);
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneRetrieveOperator.Factory simple() {
        return simple(DataPartitioning.SHARD, 10_000, 100);
    }

    private LuceneRetrieveOperator.Factory simple(DataPartitioning dataPartitioning, int size, int limit) {
        int commitEvery = Math.max(1, size / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < size; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        int taskConcurrency = 0;
        int maxPageSize = between(10, Math.max(10, size));
        return new LuceneRetrieveOperator.Factory(
            List.of(ctx),
            queryFunction,
            new ArrayList<>(),
            new ArrayList<>(),
            dataPartitioning,
            taskConcurrency,
            maxPageSize,
            limit
        );
    }

    @Override
    protected String expectedToStringOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return "LuceneRetrieveOperator[shardId=0, maxPageSize=**random**]";
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return """
            LuceneRetrieveOperator[dataPartitioning = SHARD, maxPageSize = **random**, limit = 100, sorts = [{"s":{"order":"asc"}}]]""";
    }

    // TODO tests for the other data partitioning configurations

    public void testShardDataPartitioning() {
        testShardDataPartitioning(driverContext());
    }

    public void testShardDataPartitioningWithCranky() {
        try {
            testShardDataPartitioning(crankyDriverContext());
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testShardDataPartitioning(DriverContext context) {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        testSimple(context, size, limit);
    }

    public void testEmpty() {
        testEmpty(driverContext());
    }

    public void testEmptyWithCranky() {
        try {
            testEmpty(crankyDriverContext());
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testWithSimpleMatch() throws IOException {
        initMapping();
        initIndex();
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        SearchExecutionContext sec = getSearchExecutionContext(ctx.searcher());

        testWithQuery(
            QueryBuilders.matchQuery("stored_text", "time").toQuery(sec),
            new ArrayList<>(),
            new ArrayList<>(),
            List.of("3", "4", "1", "2"),
            ctx
        );
    }

    public void testWithMatchAndRescore() throws IOException {
        initMapping();
        initIndex();
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        SearchExecutionContext sec = getSearchExecutionContext(ctx.searcher());

        Query query = QueryBuilders.matchQuery("stored_text", "time wheel").toQuery(sec);
        Rescorer firstRescorer = new QueryRescorer(query) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                return secondPassScore;
            }
        };

        Rescorer secondRescorer = new QueryRescorer(query) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                // this will flip the order of the results
                return 10000 - secondPassScore;
            }
        };

        testWithQuery(
            QueryBuilders.matchQuery("stored_text", "time").toQuery(sec),
            List.of(
                new Tuple<>(firstRescorer, 3),
                new Tuple<>(secondRescorer, 2)
            ),
            new ArrayList<>(),
            List.of("1", "3"),
            ctx
        );
    }

    public void testEverything() throws IOException {
        initMapping();
        initIndex();
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        SearchExecutionContext sec = getSearchExecutionContext(ctx.searcher());

        Query query = QueryBuilders.matchQuery("stored_text", "time wheel").toQuery(sec);
        Rescorer firstRescorer = new QueryRescorer(query) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                return secondPassScore;
            }
        };

        Rescorer secondRescorer = new QueryRescorer(query) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                // this will flip the order of the results
                return 10000 - secondPassScore;
            }
        };

        Query featureQuery1 = QueryBuilders.matchQuery("stored_text", "history").toQuery(sec);
        Query featureQuery2 = QueryBuilders.matchQuery("stored_text", "kill").toQuery(sec);

        testWithQuery(
            QueryBuilders.matchQuery("stored_text", "time").toQuery(sec),
            List.of(
                new Tuple<>(firstRescorer, 3),
                new Tuple<>(secondRescorer, 2)
            ),
           List.of(
                new Tuple<>("my_feature1", featureQuery1),
                new Tuple<>("my_feature2", featureQuery2)
            ),
            List.of("1", "3"),
            ctx
        );
    }

    private void testWithQuery(
        Query query,
        List<Tuple<Rescorer, Integer>> rescorers,
        List<Tuple<String, Query>> features,
        List<String> expectedOrderedIds,
        ShardContext shardContext
    ) throws IOException {
        int size = 4;
        int limit = 100;
        DriverContext driverContext = driverContext();
        LuceneRetrieveOperator.Factory factory = getFactory(
            shardContext,
            DataPartitioning.SHARD,
            10_000,
            100,
            query,
            rescorers,
            features
        );

        Operator op =  new ValuesSourceReaderOperator.Factory(
            List.of(
                ValuesSourceReaderOperatorTests.fieldInfo(storedTextField("stored_text"), ElementType.BYTES_REF),
                ValuesSourceReaderOperatorTests.fieldInfo(storedTextField("_score"), ElementType.DOUBLE),
                ValuesSourceReaderOperatorTests.fieldInfo(mapperService.fieldType("_id"), ElementType.BYTES_REF)
                ),
            List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
            0
        ).get(driverContext);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(driverContext, factory.get(driverContext), List.of(op), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(driverContext);

        long expectedS = 0;
        double prevScore = Double.MAX_VALUE;
        for (Page page : results) {
            BytesRefBlock idsBlock = page.getBlock(3);
            DoubleBlock scoresBlock = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                var id = idsBlock.getBytesRef(i, new BytesRef()).utf8ToString();
                assertEquals(id, expectedOrderedIds.get(i));
                var score = scoresBlock.getDouble(i);
                assertTrue(score <= prevScore);
                prevScore = score;
            }
        }
        int pages = (int) Math.ceil((float) Math.min(size, limit) / factory.maxPageSize());
        assertThat(results, hasSize(pages));
    }

    private TextFieldMapper.TextFieldType storedTextField(String name) {
        return new TextFieldMapper.TextFieldType(
            name,
            true,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            false,
            null,
            Map.of(),
            false,
            true
        );
    }

    private void testEmpty(DriverContext context) {
        testSimple(context, 0, between(10, 10_000));
    }

    private void testSimple(DriverContext ctx, int size, int limit) {
        LuceneRetrieveOperator.Factory factory = simple(DataPartitioning.SHARD, size, limit);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, S_FIELD, ElementType.LONG);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        long expectedS = 0;
        for (Page page : results) {
            if (limit - expectedS < factory.maxPageSize()) {
                assertThat(page.getPositionCount(), equalTo((int) (limit - expectedS)));
            } else {
                assertThat(page.getPositionCount(), equalTo(factory.maxPageSize()));
            }
            LongBlock sBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getLong(sBlock.getFirstValueIndex(p)), equalTo(expectedS++));
            }
        }
        int pages = (int) Math.ceil((float) Math.min(size, limit) / factory.maxPageSize());
        assertThat(results, hasSize(pages));
    }

    private MapperService mapperService;

    private void initMapping() throws IOException {
        mapperService = new MapperServiceTestCase() {
        }.createMapperService(MapperServiceTestCase.mapping(b -> {
            b.startObject("stored_text");
            b.field("type", "text");
            b.endObject();
            b.startObject("page_count");
            b.field("type", "long");
            b.endObject();
        }));
    }

    private void initIndex() {
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            writer.addDocument(
                Arrays.asList(
                    IdFieldMapper.standardIdField("1"),
                    new TextField("stored_text", "A brief history of time", Field.Store.YES),
                    new NumericDocValuesField("page_count", 250)
                )
            );
            writer.addDocument(
                Arrays.asList(
                    IdFieldMapper.standardIdField("2"),
                    new TextField("stored_text", "In search of lost time", Field.Store.YES),
                    new NumericDocValuesField("page_count", 4200)
                )
            );
            writer.addDocument(
                Arrays.asList(
                    IdFieldMapper.standardIdField("3"),
                    new TextField("stored_text", "A time to kill", Field.Store.YES),
                    new NumericDocValuesField("page_count", 650)
                )
            );
            writer.addDocument(
                Arrays.asList(
                    IdFieldMapper.standardIdField("4"),
                    new TextField("stored_text", "The wheel of time", Field.Store.YES),
                    new NumericDocValuesField("page_count", 12_000)
                )
            );
            writer.commit();
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private SearchExecutionContext getSearchExecutionContext(IndexSearcher searcher) {
        Settings settings = Settings.builder().build();
        IndexMetadata indexMetadata = IndexMetadata.builder(mapperService.getIndexSettings().getIndexMetadata()).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        final SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        final long nowInMillis = randomNonNegativeLong();
        return new SearchExecutionContext(0, 0, indexSettings, new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        }),
            (ft, fdc) -> ft.fielddataBuilder(fdc).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            mapperService,
            mapperService.mappingLookup(),
            similarityService,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            searcher,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap()
        );
    }

    private LuceneRetrieveOperator.Factory getFactory(
        ShardContext ctx,
        DataPartitioning dataPartitioning,
        int size,
        int limit,
        Query query,
        List<Tuple<Rescorer, Integer>> rescorers,
        List<Tuple<String, Query>> features
        ) throws IOException {
        Function<ShardContext, Query> queryFunction = c -> query;
        int taskConcurrency = 0;
        int maxPageSize = between(10, Math.max(10, size));
        return new LuceneRetrieveOperator.Factory(
            List.of(ctx),
            queryFunction,
            rescorers,
            features,
            dataPartitioning,
            taskConcurrency,
            maxPageSize,
            limit
        );
    }
}
