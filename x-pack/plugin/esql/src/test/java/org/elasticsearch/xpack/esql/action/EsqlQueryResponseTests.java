/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.UnsupportedEsFieldTests;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateNanosToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIP;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToSpatial;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToVersion;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class EsqlQueryResponseTests extends AbstractChunkedSerializingTestCase<EsqlQueryResponse> {
    private BlockFactory blockFactory;

    @Before
    public void newBlockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        blockFactory = new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
    }

    @After
    public void blockFactoryEmpty() {
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(AbstractPageMappingOperator.Status.ENTRY));
    }

    @Override
    protected EsqlQueryResponse createXContextTestInstance(XContentType xContentType) {
        // columnar param can't be different from the default value (false) since the EsqlQueryResponse will be serialized (by some random
        // XContentType, not to a StreamOutput) and parsed back, which doesn't preserve columnar field's value.
        return randomResponse(false, null);
    }

    @Override
    protected EsqlQueryResponse createTestInstance() {
        return randomResponse(randomBoolean(), randomProfile());
    }

    EsqlQueryResponse randomResponse(boolean columnar, EsqlQueryResponse.Profile profile) {
        return randomResponseAsync(columnar, profile, false);
    }

    EsqlQueryResponse randomResponseAsync(boolean columnar, EsqlQueryResponse.Profile profile, boolean async) {
        int noCols = randomIntBetween(1, 10);
        List<ColumnInfoImpl> columns = randomList(noCols, noCols, this::randomColumnInfo);
        int noPages = randomIntBetween(1, 20);
        List<Page> values = randomList(noPages, noPages, () -> randomPage(columns));
        String id = null;
        boolean isRunning = false;
        if (async) {
            id = randomAlphaOfLengthBetween(1, 16);
            isRunning = randomBoolean();
        }
        return new EsqlQueryResponse(
            columns,
            values,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            profile,
            columnar,
            id,
            isRunning,
            async,
            createExecutionInfo()
        );
    }

    EsqlExecutionInfo createExecutionInfo() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
        executionInfo.overallTook(new TimeValue(5000));
        executionInfo.swapCluster(
            "",
            (k, v) -> new EsqlExecutionInfo.Cluster(
                "",
                "logs-1",
                false,
                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                10,
                10,
                3,
                0,
                null,
                new TimeValue(4444L)
            )
        );
        executionInfo.swapCluster(
            "remote1",
            (k, v) -> new EsqlExecutionInfo.Cluster(
                "remote1",
                "remote1:logs-1",
                true,
                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                12,
                12,
                5,
                0,
                null,
                new TimeValue(4999L)
            )
        );
        return executionInfo;
    }

    private ColumnInfoImpl randomColumnInfo() {
        DataType type = randomValueOtherThanMany(
            t -> false == DataType.isPrimitiveAndSupported(t)
                || t == DataType.DATE_PERIOD
                || t == DataType.TIME_DURATION
                || t == DataType.PARTIAL_AGG
                || t == DataType.AGGREGATE_METRIC_DOUBLE,
            () -> randomFrom(DataType.types())
        ).widenSmallNumeric();
        return new ColumnInfoImpl(randomAlphaOfLength(10), type.esType(), randomOriginalTypes());
    }

    @Nullable
    public static List<String> randomOriginalTypes() {
        return randomBoolean() ? null : UnsupportedEsFieldTests.randomOriginalTypes();
    }

    private EsqlQueryResponse.Profile randomProfile() {
        if (randomBoolean()) {
            return null;
        }
        return new EsqlQueryResponseProfileTests().createTestInstance();
    }

    private Page randomPage(List<ColumnInfoImpl> columns) {
        return new Page(columns.stream().map(c -> {
            Block.Builder builder = PlannerUtils.toElementType(c.type()).newBlockBuilder(1, blockFactory);
            switch (c.type()) {
                case UNSIGNED_LONG, LONG, COUNTER_LONG -> ((LongBlock.Builder) builder).appendLong(randomLong());
                case INTEGER, COUNTER_INTEGER -> ((IntBlock.Builder) builder).appendInt(randomInt());
                case DOUBLE, COUNTER_DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(randomDouble());
                case KEYWORD -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                case TEXT -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(randomAlphaOfLength(10000)));
                case IP -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())))
                );
                case DATETIME -> ((LongBlock.Builder) builder).appendLong(randomInstant().toEpochMilli());
                case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(randomBoolean());
                case UNSUPPORTED -> ((BytesRefBlock.Builder) builder).appendNull();
                // TODO - add a random instant thing here?
                case DATE_NANOS -> ((LongBlock.Builder) builder).appendLong(randomNonNegativeLong());
                case VERSION -> ((BytesRefBlock.Builder) builder).appendBytesRef(new Version(randomIdentifier()).toBytesRef());
                case GEO_POINT -> ((BytesRefBlock.Builder) builder).appendBytesRef(GEO.asWkb(GeometryTestUtils.randomPoint()));
                case CARTESIAN_POINT -> ((BytesRefBlock.Builder) builder).appendBytesRef(CARTESIAN.asWkb(ShapeTestUtils.randomPoint()));
                case GEO_SHAPE -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()))
                );
                case CARTESIAN_SHAPE -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()))
                );
                case AGGREGATE_METRIC_DOUBLE -> {
                    BlockLoader.AggregateMetricDoubleBuilder aggBuilder = (BlockLoader.AggregateMetricDoubleBuilder) builder;
                    aggBuilder.min().appendDouble(randomDouble());
                    aggBuilder.max().appendDouble(randomDouble());
                    aggBuilder.sum().appendDouble(randomDouble());
                    aggBuilder.count().appendInt(randomInt());
                }
                case NULL -> builder.appendNull();
                case SOURCE -> {
                    try {
                        ((BytesRefBlock.Builder) builder).appendBytesRef(
                            BytesReference.bytes(
                                JsonXContent.contentBuilder()
                                    .startObject()
                                    .field(randomAlphaOfLength(3), randomAlphaOfLength(10))
                                    .endObject()
                            ).toBytesRef()
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                case DENSE_VECTOR -> {
                    BlockLoader.FloatBuilder floatBuilder = (BlockLoader.FloatBuilder) builder;
                    int dims = randomIntBetween(32, 64) * 2; // min 64 dims, always even
                    floatBuilder.beginPositionEntry();
                    for (int i = 0; i < dims; i++) {
                        floatBuilder.appendFloat(randomFloat());
                    }
                    floatBuilder.endPositionEntry();
                }
                // default -> throw new UnsupportedOperationException("unsupported data type [" + c + "]");
            }
            return builder.build();
        }).toArray(Block[]::new));
    }

    @Override
    protected EsqlQueryResponse mutateInstance(EsqlQueryResponse instance) {
        boolean allNull = true;
        for (ColumnInfoImpl info : instance.columns()) {
            // values inside NULL and UNSUPPORTED blocks cannot be mutated, because they are all null
            if (info.type() != DataType.NULL && info.type() != DataType.UNSUPPORTED) {
                allNull = false;
            }
        }
        List<ColumnInfoImpl> columns = instance.columns();
        List<Page> pages = deepCopyOfPages(instance);
        long documentsFound = instance.documentsFound();
        long valuesLoaded = instance.valuesLoaded();
        EsqlQueryResponse.Profile profile = instance.profile();
        boolean columnar = instance.columnar();
        boolean isAsync = instance.isAsync();
        EsqlExecutionInfo executionInfo = instance.getExecutionInfo();
        switch (allNull ? between(0, 4) : between(0, 5)) {
            case 0 -> {
                int mutCol = between(0, instance.columns().size() - 1);
                columns = new ArrayList<>(instance.columns());
                // keep the type the same so the values are still valid but change the name
                ColumnInfoImpl mut = columns.get(mutCol);
                columns.set(mutCol, new ColumnInfoImpl(mut.name() + "mut", mut.type(), mut.originalTypes()));
            }
            case 1 -> documentsFound = randomValueOtherThan(documentsFound, ESTestCase::randomNonNegativeLong);
            case 2 -> valuesLoaded = randomValueOtherThan(valuesLoaded, ESTestCase::randomNonNegativeLong);
            case 3 -> columnar = false == columnar;
            case 4 -> profile = randomValueOtherThan(profile, this::randomProfile);
            case 5 -> {
                assert allNull == false
                    : "can't replace values while preserving types if all pages are null - the only valid values are null";
                int noPages = instance.pages().size();
                List<Page> differentPages = List.of();
                do {
                    differentPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
                    differentPages = randomList(noPages, noPages, () -> randomPage(instance.columns()));
                } while (differentPages.equals(instance.pages()));
                pages.forEach(Page::releaseBlocks);
                pages = differentPages;
            }
            default -> throw new IllegalArgumentException();
        }
        ;
        return new EsqlQueryResponse(columns, pages, documentsFound, valuesLoaded, profile, columnar, isAsync, executionInfo);
    }

    private List<Page> deepCopyOfPages(EsqlQueryResponse response) {
        List<Page> deepCopiedPages = new ArrayList<>(response.pages().size());
        for (Page p : response.pages()) {
            Block[] deepCopiedBlocks = new Block[p.getBlockCount()];
            for (int b = 0; b < p.getBlockCount(); b++) {
                deepCopiedBlocks[b] = BlockUtils.deepCopyOf(p.getBlock(b), blockFactory);
            }
            deepCopiedPages.add(new Page(deepCopiedBlocks));
        }
        assertThat(deepCopiedPages, equalTo(response.pages()));
        return deepCopiedPages;
    }

    @Override
    protected Writeable.Reader<EsqlQueryResponse> instanceReader() {
        return EsqlQueryResponse.reader(blockFactory);
    }

    @Override
    protected EsqlQueryResponse doParseInstance(XContentParser parser) {
        return ResponseBuilder.fromXContent(parser);
    }

    /**
     * Used to test round tripping through x-content. Unlike lots of other
     * response objects, ESQL doesn't have production code that can parse
     * the response because it doesn't need it. But we want to test random
     * responses are valid. This helps with that by parsing it into a
     * response.
     */
    public static class ResponseBuilder {
        private static final ParseField ID = new ParseField("id");
        private static final ParseField IS_RUNNING = new ParseField("is_running");
        private static final InstantiatingObjectParser<ResponseBuilder, Void> PARSER;

        static {
            InstantiatingObjectParser.Builder<ResponseBuilder, Void> parser = InstantiatingObjectParser.builder(
                "esql/query_response",
                true,
                ResponseBuilder.class
            );
            parser.declareString(optionalConstructorArg(), ID);
            parser.declareField(
                optionalConstructorArg(),
                p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? false : p.booleanValue(),
                IS_RUNNING,
                ObjectParser.ValueType.BOOLEAN_OR_NULL
            );
            parser.declareInt(constructorArg(), new ParseField("took"));
            parser.declareLong(constructorArg(), new ParseField("documents_found"));
            parser.declareLong(constructorArg(), new ParseField("values_loaded"));
            parser.declareObjectArray(constructorArg(), (p, c) -> ColumnInfoImpl.fromXContent(p), new ParseField("columns"));
            parser.declareField(constructorArg(), (p, c) -> p.list(), new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
            parser.declareObject(optionalConstructorArg(), (p, c) -> parseClusters(p), new ParseField("_clusters"));
            PARSER = parser.build();
        }

        // Used for XContent reconstruction
        private final EsqlQueryResponse response;

        @ParserConstructor
        public ResponseBuilder(
            @Nullable String asyncExecutionId,
            Boolean isRunning,
            Integer took,
            long documentsFound,
            long valuesLoaded,
            List<ColumnInfoImpl> columns,
            List<List<Object>> values,
            EsqlExecutionInfo executionInfo
        ) {
            executionInfo.overallTook(new TimeValue(took));
            this.response = new EsqlQueryResponse(
                columns,
                List.of(valuesToPage(TestBlockFactory.getNonBreakingInstance(), columns, values)),
                documentsFound,
                valuesLoaded,
                null,
                false,
                asyncExecutionId,
                isRunning != null,
                isAsync(asyncExecutionId, isRunning),
                executionInfo
            );
        }

        static EsqlExecutionInfo parseClusters(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            int total = -1;
            int successful = -1;
            int skipped = -1;
            int running = 0;
            int partial = 0;
            int failed = 0;
            ConcurrentMap<String, EsqlExecutionInfo.Cluster> clusterInfoMap = ConcurrentCollections.newConcurrentMap();
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (EsqlExecutionInfo.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        total = parser.intValue();
                    } else if (EsqlExecutionInfo.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        successful = parser.intValue();
                    } else if (EsqlExecutionInfo.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        skipped = parser.intValue();
                    } else if (EsqlExecutionInfo.RUNNING_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        running = parser.intValue();
                    } else if (EsqlExecutionInfo.PARTIAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        partial = parser.intValue();
                    } else if (EsqlExecutionInfo.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        failed = parser.intValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (EsqlExecutionInfo.DETAILS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        String currentDetailsFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentDetailsFieldName = parser.currentName();  // cluster alias
                                if (currentDetailsFieldName.equals(EsqlExecutionInfo.LOCAL_CLUSTER_NAME_REPRESENTATION)) {
                                    currentDetailsFieldName = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                EsqlExecutionInfo.Cluster c = parseCluster(currentDetailsFieldName, parser);
                                clusterInfoMap.put(currentDetailsFieldName, c);
                            } else {
                                parser.skipChildren();
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else {
                    parser.skipChildren();
                }
            }
            if (clusterInfoMap.isEmpty()) {
                return new EsqlExecutionInfo(true);
            } else {
                return new EsqlExecutionInfo(clusterInfoMap, true);
            }
        }

        private static EsqlExecutionInfo.Cluster parseCluster(String clusterAlias, XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

            String indexExpression = null;
            String status = "running";
            long took = -1L;
            // these are all from the _shards section
            int totalShards = -1;
            int successfulShards = -1;
            int skippedShards = -1;
            int failedShards = -1;

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (EsqlExecutionInfo.Cluster.INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        indexExpression = parser.text();
                    } else if (EsqlExecutionInfo.Cluster.STATUS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        status = parser.text();
                    } else if (EsqlExecutionInfo.TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                        took = parser.longValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                failedShards = parser.intValue();
                            } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            }

            Integer totalShardsFinal = totalShards == -1 ? null : totalShards;
            Integer successfulShardsFinal = successfulShards == -1 ? null : successfulShards;
            Integer skippedShardsFinal = skippedShards == -1 ? null : skippedShards;
            Integer failedShardsFinal = failedShards == -1 ? null : failedShards;
            TimeValue tookTimeValue = took == -1L ? null : new TimeValue(took);
            return new EsqlExecutionInfo.Cluster(
                clusterAlias,
                indexExpression,
                true,
                EsqlExecutionInfo.Cluster.Status.valueOf(status.toUpperCase(Locale.ROOT)),
                totalShardsFinal,
                successfulShardsFinal,
                skippedShardsFinal,
                failedShardsFinal,
                null,
                tookTimeValue
            );
        }

        static boolean isAsync(@Nullable String asyncExecutionId, Boolean isRunning) {
            if (asyncExecutionId != null || isRunning != null) {
                return true;
            }
            return false;
        }

        static EsqlQueryResponse fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null).response;
        }
    }

    public static int clusterDetailsSize(int numClusters) {
        /* Example:
        "_clusters" : {
            "total" : 2,
            "successful" : 2,
            "running" : 0,
            "skipped" : 0,
            "partial" : 0,
            "failed" : 0,
            "details" : {
                "(local)" : {
                    "status" : "successful",
                    "indices" : "logs-1",
                    "took" : 4444,
                    "_shards" : {
                      "total" : 10,
                      "successful" : 10,
                      "skipped" : 3,
                      "failed" : 0
                    }
                },
                "remote1" : {
                    "status" : "successful",
                    "indices" : "remote1:logs-1",
                    "took" : 4999,
                    "_shards" : {
                      "total" : 12,
                      "successful" : 12,
                      "skipped" : 5,
                      "failed" : 0
                    }
                }
            }
         }
         */
        return numClusters * 4 + 1;
    }

    public void testChunkResponseSizeColumnar() {
        try (EsqlQueryResponse resp = randomResponse(true, null)) {
            int columnCount = resp.pages().get(0).getBlockCount();
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount() * p.getBlockCount()).sum() + columnCount * 2;
            assertChunkCount(resp, r -> 6 + clusterDetailsSize(resp.getExecutionInfo().clusterInfo.size()) + bodySize);
        }

        try (EsqlQueryResponse resp = randomResponseAsync(true, null, true)) {
            int columnCount = resp.pages().get(0).getBlockCount();
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount() * p.getBlockCount()).sum() + columnCount * 2;
            assertChunkCount(resp, r -> 7 + clusterDetailsSize(resp.getExecutionInfo().clusterInfo.size()) + bodySize); // is_running
        }
    }

    public void testChunkResponseSizeRows() {
        try (EsqlQueryResponse resp = randomResponse(false, null)) {
            int bodySize = resp.pages().stream().mapToInt(Page::getPositionCount).sum();
            assertChunkCount(resp, r -> 6 + clusterDetailsSize(resp.getExecutionInfo().clusterInfo.size()) + bodySize);
        }
        try (EsqlQueryResponse resp = randomResponseAsync(false, null, true)) {
            int bodySize = resp.pages().stream().mapToInt(Page::getPositionCount).sum();
            assertChunkCount(resp, r -> 7 + clusterDetailsSize(resp.getExecutionInfo().clusterInfo.size()) + bodySize);
        }
    }

    public void testSimpleXContentColumnar() {
        try (EsqlQueryResponse response = simple(true)) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "documents_found" : 3,
                  "values_loaded" : 100,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "integer"
                    }
                  ],
                  "values" : [
                    [
                      40,
                      80
                    ]
                  ]
                }"""));
        }
    }

    public void testSimpleXContentColumnarDropNulls() {
        try (EsqlQueryResponse response = simple(true)) {
            assertThat(
                Strings.toString(
                    wrapAsToXContent(response),
                    new ToXContent.MapParams(Map.of(DROP_NULL_COLUMNS_OPTION, "true")),
                    true,
                    false
                ),
                equalTo("""
                    {
                      "documents_found" : 3,
                      "values_loaded" : 100,
                      "all_columns" : [
                        {
                          "name" : "foo",
                          "type" : "integer"
                        }
                      ],
                      "columns" : [
                        {
                          "name" : "foo",
                          "type" : "integer"
                        }
                      ],
                      "values" : [
                        [
                          40,
                          80
                        ]
                      ]
                    }""")
            );
        }
    }

    public void testSimpleXContentColumnarAsync() {
        try (EsqlQueryResponse response = simple(true, true)) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "is_running" : false,
                  "documents_found" : 3,
                  "values_loaded" : 100,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "integer"
                    }
                  ],
                  "values" : [
                    [
                      40,
                      80
                    ]
                  ]
                }"""));
        }
    }

    public void testSimpleXContentRows() {
        try (EsqlQueryResponse response = simple(false)) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "documents_found" : 3,
                  "values_loaded" : 100,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "integer"
                    }
                  ],
                  "values" : [
                    [
                      40
                    ],
                    [
                      80
                    ]
                  ]
                }"""));
        }
    }

    public void testSimpleXContentRowsAsync() {
        try (EsqlQueryResponse response = simple(false, true)) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "is_running" : false,
                  "documents_found" : 3,
                  "values_loaded" : 100,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "integer"
                    }
                  ],
                  "values" : [
                    [
                      40
                    ],
                    [
                      80
                    ]
                  ]
                }"""));
        }
    }

    public void testBasicXContentIdAndRunning() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfoImpl("foo", "integer", null)),
                List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
                10,
                99,
                null,
                false,
                "id-123",
                true,
                true,
                null
            )
        ) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "id" : "id-123",
                  "is_running" : true,
                  "documents_found" : 10,
                  "values_loaded" : 99,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "integer"
                    }
                  ],
                  "values" : [
                    [
                      40
                    ],
                    [
                      80
                    ]
                  ]
                }"""));
        }
    }

    public void testXContentOriginalTypes() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfoImpl("foo", "unsupported", List.of("foo", "bar"))),
                List.of(new Page(blockFactory.newConstantNullBlock(2))),
                1,
                1,
                null,
                false,
                null,
                false,
                false,
                null
            )
        ) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "documents_found" : 1,
                  "values_loaded" : 1,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "unsupported",
                      "original_types" : [
                        "foo",
                        "bar"
                      ]
                    }
                  ],
                  "values" : [
                    [
                      null
                    ],
                    [
                      null
                    ]
                  ]
                }"""));
        }
    }

    public void testNullColumnsXContentDropNulls() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfoImpl("foo", "integer", null), new ColumnInfoImpl("all_null", "integer", null)),
                List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock(), blockFactory.newConstantNullBlock(2))),
                1,
                3,
                null,
                false,
                null,
                false,
                false,
                null
            )
        ) {
            assertThat(
                Strings.toString(
                    wrapAsToXContent(response),
                    new ToXContent.MapParams(Map.of(DROP_NULL_COLUMNS_OPTION, "true")),
                    true,
                    false
                ),
                equalTo("""
                    {
                      "documents_found" : 1,
                      "values_loaded" : 3,
                      "all_columns" : [
                        {
                          "name" : "foo",
                          "type" : "integer"
                        },
                        {
                          "name" : "all_null",
                          "type" : "integer"
                        }
                      ],
                      "columns" : [
                        {
                          "name" : "foo",
                          "type" : "integer"
                        }
                      ],
                      "values" : [
                        [
                          40
                        ],
                        [
                          80
                        ]
                      ]
                    }""")
            );
        }
    }

    /**
     * This is a paranoid test to make sure the {@link Block}s produced by {@link Block.Builder}
     * that contain only {@code null} entries are properly recognized by the {@link EsqlQueryResponse#DROP_NULL_COLUMNS_OPTION}.
     */
    public void testNullColumnsFromBuilderXContentDropNulls() {
        try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(2)) {
            b.appendNull();
            b.appendNull();
            try (
                EsqlQueryResponse response = new EsqlQueryResponse(
                    List.of(new ColumnInfoImpl("foo", "integer", null), new ColumnInfoImpl("all_null", "integer", null)),
                    List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock(), b.build())),
                    1,
                    3,
                    null,
                    false,
                    null,
                    false,
                    false,
                    null
                )
            ) {
                assertThat(
                    Strings.toString(
                        wrapAsToXContent(response),
                        new ToXContent.MapParams(Map.of(DROP_NULL_COLUMNS_OPTION, "true")),
                        true,
                        false
                    ),
                    equalTo("""
                        {
                          "documents_found" : 1,
                          "values_loaded" : 3,
                          "all_columns" : [
                            {
                              "name" : "foo",
                              "type" : "integer"
                            },
                            {
                              "name" : "all_null",
                              "type" : "integer"
                            }
                          ],
                          "columns" : [
                            {
                              "name" : "foo",
                              "type" : "integer"
                            }
                          ],
                          "values" : [
                            [
                              40
                            ],
                            [
                              80
                            ]
                          ]
                        }""")
                );
            }
        }
    }

    private EsqlQueryResponse simple(boolean columnar) {
        return simple(columnar, false);
    }

    private EsqlQueryResponse simple(boolean columnar, boolean async) {
        return new EsqlQueryResponse(
            List.of(new ColumnInfoImpl("foo", "integer", null)),
            List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
            3,
            100,
            null,
            columnar,
            async,
            null
        );
    }

    public void testProfileXContent() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfoImpl("foo", "integer", null)),
                List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
                10,
                100,
                new EsqlQueryResponse.Profile(
                    List.of(
                        new DriverProfile(
                            "test",
                            "elasticsearch",
                            "node-1",
                            1723489812649L,
                            1723489819929L,
                            20021,
                            20000,
                            12,
                            List.of(new OperatorStatus("asdf", new AbstractPageMappingOperator.Status(10021, 10, 111, 222))),
                            DriverSleeps.empty()
                        )
                    ),
                    List.of(new PlanProfile("test", "elasticsearch", "node-1", "plan tree"))
                ),
                false,
                false,
                null
            );
        ) {
            assertThat(Strings.toString(wrapAsToXContent(response), true, false), equalTo("""
                {
                  "documents_found" : 10,
                  "values_loaded" : 100,
                  "columns" : [
                    {
                      "name" : "foo",
                      "type" : "integer"
                    }
                  ],
                  "values" : [
                    [
                      40
                    ],
                    [
                      80
                    ]
                  ],
                  "profile" : {
                    "drivers" : [
                      {
                        "description" : "test",
                        "cluster_name" : "elasticsearch",
                        "node_name" : "node-1",
                        "start_millis" : 1723489812649,
                        "stop_millis" : 1723489819929,
                        "took_nanos" : 20021,
                        "cpu_nanos" : 20000,
                        "documents_found" : 0,
                        "values_loaded" : 0,
                        "iterations" : 12,
                        "operators" : [
                          {
                            "operator" : "asdf",
                            "status" : {
                              "process_nanos" : 10021,
                              "pages_processed" : 10,
                              "rows_received" : 111,
                              "rows_emitted" : 222
                            }
                          }
                        ],
                        "sleeps" : {
                          "counts" : { },
                          "first" : [ ],
                          "last" : [ ]
                        }
                      }
                    ],
                    "plans" : [
                      {
                        "description" : "test",
                        "cluster_name" : "elasticsearch",
                        "node_name" : "node-1",
                        "plan" : "plan tree"
                      }
                    ]
                  }
                }"""));
        }
    }

    @Override
    protected void dispose(EsqlQueryResponse esqlQueryResponse) {
        esqlQueryResponse.close();
    }

    // Tests for response::column
    public void testColumns() {
        var intBlk1 = blockFactory.newIntArrayVector(new int[] { 10, 20 }, 2).asBlock();
        var intBlk2 = blockFactory.newIntArrayVector(new int[] { 30, 40, 50 }, 3).asBlock();
        var longBlk1 = blockFactory.newLongArrayVector(new long[] { 100L, 200L }, 2).asBlock();
        var longBlk2 = blockFactory.newLongArrayVector(new long[] { 300L, 400L, 500L }, 3).asBlock();
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer", null), new ColumnInfoImpl("bar", "long", null));
        var pages = List.of(new Page(intBlk1, longBlk1), new Page(intBlk2, longBlk2));
        try (var response = new EsqlQueryResponse(columnInfo, pages, 0, 0, null, false, null, false, false, null)) {
            assertThat(columnValues(response.column(0)), contains(10, 20, 30, 40, 50));
            assertThat(columnValues(response.column(1)), contains(100L, 200L, 300L, 400L, 500L));
            expectThrows(IllegalArgumentException.class, () -> response.column(-1));
            expectThrows(IllegalArgumentException.class, () -> response.column(2));
        }
    }

    public void testColumnsIllegalArg() {
        var intBlk1 = blockFactory.newIntArrayVector(new int[] { 10 }, 1).asBlock();
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer", null));
        var pages = List.of(new Page(intBlk1));
        try (var response = new EsqlQueryResponse(columnInfo, pages, 0, 0, null, false, null, false, false, null)) {
            expectThrows(IllegalArgumentException.class, () -> response.column(-1));
            expectThrows(IllegalArgumentException.class, () -> response.column(1));
        }
    }

    public void testColumnsWithNull() {
        IntBlock blk1, blk2, blk3;
        try (
            var bb1 = blockFactory.newIntBlockBuilder(2);
            var bb2 = blockFactory.newIntBlockBuilder(4);
            var bb3 = blockFactory.newIntBlockBuilder(4)
        ) {
            blk1 = bb1.appendInt(10).appendNull().build();
            blk2 = bb2.appendInt(30).appendNull().appendNull().appendInt(60).build();
            blk3 = bb3.appendNull().appendInt(80).appendInt(90).appendNull().build();
        }
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer", null));
        var pages = List.of(new Page(blk1), new Page(blk2), new Page(blk3));
        try (var response = new EsqlQueryResponse(columnInfo, pages, 0, 0, null, false, null, false, false, null)) {
            assertThat(columnValues(response.column(0)), contains(10, null, 30, null, null, 60, null, 80, 90, null));
            expectThrows(IllegalArgumentException.class, () -> response.column(-1));
            expectThrows(IllegalArgumentException.class, () -> response.column(2));
        }
    }

    public void testColumnsWithMultiValue() {
        IntBlock blk1, blk2, blk3;
        try (
            var bb1 = blockFactory.newIntBlockBuilder(2);
            var bb2 = blockFactory.newIntBlockBuilder(4);
            var bb3 = blockFactory.newIntBlockBuilder(4)
        ) {
            blk1 = bb1.beginPositionEntry().appendInt(10).appendInt(20).endPositionEntry().appendNull().build();
            blk2 = bb2.beginPositionEntry().appendInt(40).appendInt(50).endPositionEntry().build();
            blk3 = bb3.appendNull().appendInt(70).appendInt(80).appendNull().build();
        }
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer", null));
        var pages = List.of(new Page(blk1), new Page(blk2), new Page(blk3));
        try (var response = new EsqlQueryResponse(columnInfo, pages, 0, 0, null, false, null, false, false, null)) {
            assertThat(columnValues(response.column(0)), contains(List.of(10, 20), null, List.of(40, 50), null, 70, 80, null));
            expectThrows(IllegalArgumentException.class, () -> response.column(-1));
            expectThrows(IllegalArgumentException.class, () -> response.column(2));
        }
    }

    public void testRowValues() {
        for (int times = 0; times < 10; times++) {
            int numColumns = randomIntBetween(1, 10);
            List<ColumnInfoImpl> columns = randomList(numColumns, numColumns, this::randomColumnInfo);
            int noPages = randomIntBetween(1, 20);
            List<Page> pages = randomList(noPages, noPages, () -> randomPage(columns));
            try (var resp = new EsqlQueryResponse(columns, pages, 0, 0, null, false, "", false, false, null)) {
                var rowValues = getValuesList(resp.rows());
                var valValues = getValuesList(resp.values());
                for (int i = 0; i < rowValues.size(); i++) {
                    assertThat(rowValues.get(i), equalTo(valValues.get(i)));
                }
            }
        }
    }

    static List<List<Object>> getValuesList(Iterator<Iterator<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEachRemaining(row -> {
            var rowValues = new ArrayList<>();
            row.forEachRemaining(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    static List<List<Object>> getValuesList(Iterable<Iterable<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEach(row -> {
            var rowValues = new ArrayList<>();
            row.forEach(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    static List<Object> columnValues(Iterator<Object> values) {
        List<Object> l = new ArrayList<>();
        values.forEachRemaining(l::add);
        return l;
    }

    /**
     * Converts a list of values to Pages so that we can parse from xcontent, so we
     * can test round tripping. This is functionally the inverse of {@link PositionToXContent}.
     */
    static Page valuesToPage(BlockFactory blockFactory, List<ColumnInfoImpl> columns, List<List<Object>> values) {
        List<DataType> dataTypes = columns.stream().map(ColumnInfoImpl::type).toList();
        List<Block.Builder> results = dataTypes.stream()
            .map(c -> PlannerUtils.toElementType(c).newBlockBuilder(values.size(), blockFactory))
            .toList();

        for (List<Object> row : values) {
            for (int c = 0; c < row.size(); c++) {
                var builder = results.get(c);
                var value = row.get(c);
                switch (dataTypes.get(c)) {
                    case UNSIGNED_LONG -> ((LongBlock.Builder) builder).appendLong(longToUnsignedLong(((Number) value).longValue(), true));
                    case LONG, COUNTER_LONG -> ((LongBlock.Builder) builder).appendLong(((Number) value).longValue());
                    case INTEGER, COUNTER_INTEGER -> ((IntBlock.Builder) builder).appendInt(((Number) value).intValue());
                    case DOUBLE, COUNTER_DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(((Number) value).doubleValue());
                    case KEYWORD, TEXT -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(value.toString()));
                    case UNSUPPORTED -> ((BytesRefBlock.Builder) builder).appendNull();
                    case IP -> ((BytesRefBlock.Builder) builder).appendBytesRef(stringToIP(value.toString()));
                    case DATETIME -> {
                        long longVal = dateTimeToLong(value.toString());
                        ((LongBlock.Builder) builder).appendLong(longVal);
                    }
                    case DATE_NANOS -> {
                        long longVal = dateNanosToLong(value.toString());
                        ((LongBlock.Builder) builder).appendLong(longVal);
                    }
                    case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(((Boolean) value));
                    case NULL -> builder.appendNull();
                    case VERSION -> ((BytesRefBlock.Builder) builder).appendBytesRef(stringToVersion(new BytesRef(value.toString())));
                    case SOURCE -> {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> o = (Map<String, ?>) value;
                        try {
                            try (XContentBuilder sourceBuilder = JsonXContent.contentBuilder()) {
                                sourceBuilder.map(o);
                                ((BytesRefBlock.Builder) builder).appendBytesRef(BytesReference.bytes(sourceBuilder).toBytesRef());
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    case GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE -> {
                        // This just converts WKT to WKB, so does not need CRS knowledge, we could merge GEO and CARTESIAN here
                        BytesRef wkb = stringToSpatial(value.toString());
                        ((BytesRefBlock.Builder) builder).appendBytesRef(wkb);
                    }
                    case AGGREGATE_METRIC_DOUBLE -> {
                        BlockLoader.AggregateMetricDoubleBuilder aggBuilder = (BlockLoader.AggregateMetricDoubleBuilder) builder;
                        aggBuilder.min().appendDouble(((Number) value).doubleValue());
                        aggBuilder.max().appendDouble(((Number) value).doubleValue());
                        aggBuilder.sum().appendDouble(((Number) value).doubleValue());
                        aggBuilder.count().appendInt(((Number) value).intValue());
                    }
                    case DENSE_VECTOR -> {
                        FloatBlock.Builder floatBuilder = (FloatBlock.Builder) builder;
                        List<?> vector = (List<?>) value;
                        floatBuilder.beginPositionEntry();
                        for (Object v : vector) {
                            switch (v) {
                                // XContentParser may retrieve Double values - we convert them to Float if needed
                                case Double d -> floatBuilder.appendFloat(d.floatValue());
                                case Float f -> floatBuilder.appendFloat(f);
                                default -> fail("Unexpected dense_vector value type: " + v.getClass());
                            }
                        }
                        floatBuilder.endPositionEntry();
                    }
                }
            }
        }
        return new Page(results.stream().map(Block.Builder::build).toArray(Block[]::new));
    }
}
