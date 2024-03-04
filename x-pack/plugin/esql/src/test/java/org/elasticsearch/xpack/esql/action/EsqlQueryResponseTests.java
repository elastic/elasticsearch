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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.action.ResponseValueUtils.valuesToPage;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;
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
        return new NamedWriteableRegistry(
            Stream.concat(Stream.of(AbstractPageMappingOperator.Status.ENTRY), Block.getNamedWriteables().stream()).toList()
        );
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
        List<ColumnInfo> columns = randomList(noCols, noCols, this::randomColumnInfo);
        int noPages = randomIntBetween(1, 20);
        List<Page> values = randomList(noPages, noPages, () -> randomPage(columns));
        String id = null;
        boolean isRunning = false;
        if (async) {
            id = randomAlphaOfLengthBetween(1, 16);
            isRunning = randomBoolean();
        }
        return new EsqlQueryResponse(columns, values, profile, columnar, id, isRunning, async);
    }

    private ColumnInfo randomColumnInfo() {
        DataType type = randomValueOtherThanMany(
            t -> false == DataTypes.isPrimitive(t) || t == EsqlDataTypes.DATE_PERIOD || t == EsqlDataTypes.TIME_DURATION,
            () -> randomFrom(EsqlDataTypes.types())
        );
        type = EsqlDataTypes.widenSmallNumericTypes(type);
        return new ColumnInfo(randomAlphaOfLength(10), type.esType());
    }

    private EsqlQueryResponse.Profile randomProfile() {
        if (randomBoolean()) {
            return null;
        }
        return new EsqlQueryResponseProfileTests().createTestInstance();
    }

    private Page randomPage(List<ColumnInfo> columns) {
        return new Page(columns.stream().map(c -> {
            Block.Builder builder = PlannerUtils.toElementType(EsqlDataTypes.fromName(c.type())).newBlockBuilder(1, blockFactory);
            switch (c.type()) {
                case "unsigned_long", "long" -> ((LongBlock.Builder) builder).appendLong(randomLong());
                case "integer" -> ((IntBlock.Builder) builder).appendInt(randomInt());
                case "double" -> ((DoubleBlock.Builder) builder).appendDouble(randomDouble());
                case "keyword" -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                case "text" -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(randomAlphaOfLength(10000)));
                case "ip" -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())))
                );
                case "date" -> ((LongBlock.Builder) builder).appendLong(randomInstant().toEpochMilli());
                case "boolean" -> ((BooleanBlock.Builder) builder).appendBoolean(randomBoolean());
                case "unsupported" -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    new BytesRef(UnsupportedValueSource.UNSUPPORTED_OUTPUT)
                );
                case "version" -> ((BytesRefBlock.Builder) builder).appendBytesRef(new Version(randomIdentifier()).toBytesRef());
                case "geo_point" -> ((BytesRefBlock.Builder) builder).appendBytesRef(GEO.asWkb(GeometryTestUtils.randomPoint()));
                case "cartesian_point" -> ((BytesRefBlock.Builder) builder).appendBytesRef(CARTESIAN.asWkb(ShapeTestUtils.randomPoint()));
                case "geo_shape" -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()))
                );
                case "cartesian_shape" -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()))
                );
                case "null" -> builder.appendNull();
                case "_source" -> {
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
                default -> throw new UnsupportedOperationException("unsupported data type [" + c + "]");
            }
            return builder.build();
        }).toArray(Block[]::new));
    }

    @Override
    protected EsqlQueryResponse mutateInstance(EsqlQueryResponse instance) {
        boolean allNull = true;
        for (ColumnInfo info : instance.columns()) {
            if (false == info.type().equals("null")) {
                allNull = false;
            }
        }
        return switch (allNull ? between(0, 2) : between(0, 3)) {
            case 0 -> {
                int mutCol = between(0, instance.columns().size() - 1);
                List<ColumnInfo> cols = new ArrayList<>(instance.columns());
                // keep the type the same so the values are still valid but change the name
                cols.set(mutCol, new ColumnInfo(cols.get(mutCol).name() + "mut", cols.get(mutCol).type()));
                yield new EsqlQueryResponse(cols, deepCopyOfPages(instance), instance.profile(), instance.columnar(), instance.isAsync());
            }
            case 1 -> new EsqlQueryResponse(
                instance.columns(),
                deepCopyOfPages(instance),
                instance.profile(),
                false == instance.columnar(),
                instance.isAsync()
            );
            case 2 -> new EsqlQueryResponse(
                instance.columns(),
                deepCopyOfPages(instance),
                randomValueOtherThan(instance.profile(), this::randomProfile),
                instance.columnar(),
                instance.isAsync()
            );
            case 3 -> {
                int noPages = instance.pages().size();
                List<Page> differentPages = List.of();
                do {
                    differentPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
                    differentPages = randomList(noPages, noPages, () -> randomPage(instance.columns()));
                } while (differentPages.equals(instance.pages()));
                yield new EsqlQueryResponse(
                    instance.columns(),
                    differentPages,
                    instance.profile(),
                    instance.columnar(),
                    instance.isAsync()
                );
            }
            default -> throw new IllegalArgumentException();
        };
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
            parser.declareObjectArray(constructorArg(), (p, c) -> ColumnInfo.fromXContent(p), new ParseField("columns"));
            parser.declareField(constructorArg(), (p, c) -> p.list(), new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
            PARSER = parser.build();
        }

        // Used for XContent reconstruction
        private final EsqlQueryResponse response;

        @ParserConstructor
        public ResponseBuilder(@Nullable String asyncExecutionId, Boolean isRunning, List<ColumnInfo> columns, List<List<Object>> values) {
            this.response = new EsqlQueryResponse(
                columns,
                List.of(valuesToPage(TestBlockFactory.getNonBreakingInstance(), columns, values)),
                null,
                false,
                asyncExecutionId,
                isRunning != null,
                isAsync(asyncExecutionId, isRunning)
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

    public void testChunkResponseSizeColumnar() {
        try (EsqlQueryResponse resp = randomResponse(true, null)) {
            int columnCount = resp.pages().get(0).getBlockCount();
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount() * p.getBlockCount()).sum() + columnCount * 2;
            assertChunkCount(resp, r -> 5 + bodySize);
        }

        try (EsqlQueryResponse resp = randomResponseAsync(true, null, true)) {
            int columnCount = resp.pages().get(0).getBlockCount();
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount() * p.getBlockCount()).sum() + columnCount * 2;
            assertChunkCount(resp, r -> 6 + bodySize); // is_running
        }
    }

    public void testChunkResponseSizeRows() {
        try (EsqlQueryResponse resp = randomResponse(false, null)) {
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount()).sum();
            assertChunkCount(resp, r -> 5 + bodySize);
        }
        try (EsqlQueryResponse resp = randomResponseAsync(false, null, true)) {
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount()).sum();
            assertChunkCount(resp, r -> 6 + bodySize);
        }
    }

    public void testSimpleXContentColumnar() {
        try (EsqlQueryResponse response = simple(true)) {
            assertThat(Strings.toString(wrapAsToXContent(response)), equalTo("""
                {"columns":[{"name":"foo","type":"integer"}],"values":[[40,80]]}"""));
        }
    }

    public void testSimpleXContentColumnarDropNulls() {
        try (EsqlQueryResponse response = simple(true)) {
            assertThat(
                Strings.toString(wrapAsToXContent(response), new ToXContent.MapParams(Map.of(DROP_NULL_COLUMNS_OPTION, "true"))),
                equalTo("""
                    {"all_columns":[{"name":"foo","type":"integer"}],"columns":[{"name":"foo","type":"integer"}],"values":[[40,80]]}""")
            );
        }
    }

    public void testSimpleXContentColumnarAsync() {
        try (EsqlQueryResponse response = simple(true, true)) {
            assertThat(Strings.toString(wrapAsToXContent(response)), equalTo("""
                {"is_running":false,"columns":[{"name":"foo","type":"integer"}],"values":[[40,80]]}"""));
        }
    }

    public void testSimpleXContentRows() {
        try (EsqlQueryResponse response = simple(false)) {
            assertThat(Strings.toString(wrapAsToXContent(response)), equalTo("""
                {"columns":[{"name":"foo","type":"integer"}],"values":[[40],[80]]}"""));
        }
    }

    public void testSimpleXContentRowsAsync() {
        try (EsqlQueryResponse response = simple(false, true)) {
            assertThat(Strings.toString(wrapAsToXContent(response)), equalTo("""
                {"is_running":false,"columns":[{"name":"foo","type":"integer"}],"values":[[40],[80]]}"""));
        }
    }

    public void testBasicXContentIdAndRunning() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfo("foo", "integer")),
                List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
                null,
                false,
                "id-123",
                true,
                true
            )
        ) {
            assertThat(Strings.toString(response), equalTo("""
                {"id":"id-123","is_running":true,"columns":[{"name":"foo","type":"integer"}],"values":[[40],[80]]}"""));
        }
    }

    public void testNullColumnsXContentDropNulls() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfo("foo", "integer"), new ColumnInfo("all_null", "integer")),
                List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock(), blockFactory.newConstantNullBlock(2))),
                null,
                false,
                null,
                false,
                false
            )
        ) {
            assertThat(
                Strings.toString(wrapAsToXContent(response), new ToXContent.MapParams(Map.of(DROP_NULL_COLUMNS_OPTION, "true"))),
                equalTo("{" + """
                    "all_columns":[{"name":"foo","type":"integer"},{"name":"all_null","type":"integer"}],""" + """
                    "columns":[{"name":"foo","type":"integer"}],""" + """
                    "values":[[40],[80]]}""")
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
                    List.of(new ColumnInfo("foo", "integer"), new ColumnInfo("all_null", "integer")),
                    List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock(), b.build())),
                    null,
                    false,
                    null,
                    false,
                    false
                )
            ) {
                assertThat(
                    Strings.toString(wrapAsToXContent(response), new ToXContent.MapParams(Map.of(DROP_NULL_COLUMNS_OPTION, "true"))),
                    equalTo("{" + """
                        "all_columns":[{"name":"foo","type":"integer"},{"name":"all_null","type":"integer"}],""" + """
                        "columns":[{"name":"foo","type":"integer"}],""" + """
                        "values":[[40],[80]]}""")
                );
            }
        }
    }

    private EsqlQueryResponse simple(boolean columnar) {
        return simple(columnar, false);
    }

    private EsqlQueryResponse simple(boolean columnar, boolean async) {
        return new EsqlQueryResponse(
            List.of(new ColumnInfo("foo", "integer")),
            List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
            null,
            columnar,
            async
        );
    }

    public void testProfileXContent() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfo("foo", "integer")),
                List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
                new EsqlQueryResponse.Profile(
                    List.of(
                        new DriverProfile(
                            20021,
                            20000,
                            12,
                            List.of(new DriverStatus.OperatorStatus("asdf", new AbstractPageMappingOperator.Status(10021, 10)))
                        )
                    )
                ),
                false,
                false
            );
        ) {
            assertThat(Strings.toString(response, true, false), equalTo("""
                {
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
                        "took_nanos" : 20021,
                        "cpu_nanos" : 20000,
                        "iterations" : 12,
                        "operators" : [
                          {
                            "operator" : "asdf",
                            "status" : {
                              "process_nanos" : 10021,
                              "pages_processed" : 10
                            }
                          }
                        ]
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
}
