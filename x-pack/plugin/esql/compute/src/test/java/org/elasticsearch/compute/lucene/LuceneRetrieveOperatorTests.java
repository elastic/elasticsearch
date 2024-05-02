package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BytesRefBlock;
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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
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
        int size = 4;
        int limit = 100;
        DriverContext ctx = driverContext();
        LuceneRetrieveOperator.Factory factory = getFactory(
            DataPartitioning.SHARD,
            10_000,
            100,
            QueryBuilders.matchQuery("stored_text", "lost")
        );

        Operator op =  new ValuesSourceReaderOperator.Factory(
            List.of(
                ValuesSourceReaderOperatorTests.fieldInfo(storedTextField("stored_text"), ElementType.BYTES_REF)
            ),
            List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
            0
        ).get(ctx);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(op), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        long expectedS = 0;
        for (Page page : results) {
            BytesRefBlock keysBlock = page.getBlock(1);
            for (int i = 0; i < page.getPositionCount(); i++) {
                var key = keysBlock.getBytesRef(i, new BytesRef()).utf8ToString();
                assertTrue(key.length() > 0);
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
            false, // TODO randomize - if the field is stored we should load from the stored field even if there is source
            null,
            Map.of(),
            false,
            true
        );
    }

    private LuceneRetrieveOperator.Factory getFactory(DataPartitioning dataPartitioning, int size, int limit, AbstractQueryBuilder queryBuilder) throws IOException {
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            writer.addDocument(
                Arrays.asList(
                    new TextField("stored_text", "A brief history of time", Field.Store.YES)
                )
            );
            writer.addDocument(
                Arrays.asList(
                    new TextField("stored_text", "In search of lost time", Field.Store.YES)
                )
            );
            writer.addDocument(
                Arrays.asList(
                    new TextField("stored_text", "A time to kill", Field.Store.YES)
                )
            );
            writer.addDocument(
                Arrays.asList(
                    new TextField("stored_text", "The wheel of time", Field.Store.YES)
                )
            );
            writer.commit();
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        SearchExecutionContext sec = getSearchExecutionContext(ctx.searcher());
        Query query = queryBuilder.toQuery(sec);
        Function<ShardContext, Query> queryFunction = c -> query;
        int taskConcurrency = 0;
        int maxPageSize = between(10, Math.max(10, size));
        return new LuceneRetrieveOperator.Factory(
            List.of(ctx),
            queryFunction,
            dataPartitioning,
            taskConcurrency,
            maxPageSize,
            limit
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
        }));
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
}
