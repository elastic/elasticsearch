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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
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
        List<ColumnInfoImpl> columns = randomList(noCols, noCols, this::randomColumnInfo);
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

    private ColumnInfoImpl randomColumnInfo() {
        DataType type = randomValueOtherThanMany(
            t -> false == DataType.isPrimitiveAndSupported(t)
                || t == DataType.DATE_PERIOD
                || t == DataType.TIME_DURATION
                || t == DataType.PARTIAL_AGG,
            () -> randomFrom(DataType.types())
        ).widenSmallNumeric();
        return new ColumnInfoImpl(randomAlphaOfLength(10), type.esType());
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
                case UNSUPPORTED -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    new BytesRef(UnsupportedValueSource.UNSUPPORTED_OUTPUT)
                );
                case VERSION -> ((BytesRefBlock.Builder) builder).appendBytesRef(new Version(randomIdentifier()).toBytesRef());
                case GEO_POINT -> ((BytesRefBlock.Builder) builder).appendBytesRef(GEO.asWkb(GeometryTestUtils.randomPoint()));
                case CARTESIAN_POINT -> ((BytesRefBlock.Builder) builder).appendBytesRef(CARTESIAN.asWkb(ShapeTestUtils.randomPoint()));
                case GEO_SHAPE -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()))
                );
                case CARTESIAN_SHAPE -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                    CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()))
                );
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
                // default -> throw new UnsupportedOperationException("unsupported data type [" + c + "]");
            }
            return builder.build();
        }).toArray(Block[]::new));
    }

    @Override
    protected EsqlQueryResponse mutateInstance(EsqlQueryResponse instance) {
        boolean allNull = true;
        for (ColumnInfoImpl info : instance.columns()) {
            if (info.type() != DataType.NULL) {
                allNull = false;
            }
        }
        return switch (allNull ? between(0, 2) : between(0, 3)) {
            case 0 -> {
                int mutCol = between(0, instance.columns().size() - 1);
                List<ColumnInfoImpl> cols = new ArrayList<>(instance.columns());
                // keep the type the same so the values are still valid but change the name
                cols.set(mutCol, new ColumnInfoImpl(cols.get(mutCol).name() + "mut", cols.get(mutCol).type()));
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
            parser.declareObjectArray(constructorArg(), (p, c) -> ColumnInfoImpl.fromXContent(p), new ParseField("columns"));
            parser.declareField(constructorArg(), (p, c) -> p.list(), new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
            PARSER = parser.build();
        }

        // Used for XContent reconstruction
        private final EsqlQueryResponse response;

        @ParserConstructor
        public ResponseBuilder(
            @Nullable String asyncExecutionId,
            Boolean isRunning,
            List<ColumnInfoImpl> columns,
            List<List<Object>> values
        ) {
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
                List.of(new ColumnInfoImpl("foo", "integer")),
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
                List.of(new ColumnInfoImpl("foo", "integer"), new ColumnInfoImpl("all_null", "integer")),
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
                    List.of(new ColumnInfoImpl("foo", "integer"), new ColumnInfoImpl("all_null", "integer")),
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
            List.of(new ColumnInfoImpl("foo", "integer")),
            List.of(new Page(blockFactory.newIntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
            null,
            columnar,
            async
        );
    }

    public void testProfileXContent() {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfoImpl("foo", "integer")),
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

    // Tests for response::column
    public void testColumns() {
        var intBlk1 = blockFactory.newIntArrayVector(new int[] { 10, 20 }, 2).asBlock();
        var intBlk2 = blockFactory.newIntArrayVector(new int[] { 30, 40, 50 }, 3).asBlock();
        var longBlk1 = blockFactory.newLongArrayVector(new long[] { 100L, 200L }, 2).asBlock();
        var longBlk2 = blockFactory.newLongArrayVector(new long[] { 300L, 400L, 500L }, 3).asBlock();
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer"), new ColumnInfoImpl("bar", "long"));
        var pages = List.of(new Page(intBlk1, longBlk1), new Page(intBlk2, longBlk2));
        try (var response = new EsqlQueryResponse(columnInfo, pages, null, false, null, false, false)) {
            assertThat(columnValues(response.column(0)), contains(10, 20, 30, 40, 50));
            assertThat(columnValues(response.column(1)), contains(100L, 200L, 300L, 400L, 500L));
            expectThrows(IllegalArgumentException.class, () -> response.column(-1));
            expectThrows(IllegalArgumentException.class, () -> response.column(2));
        }
    }

    public void testColumnsIllegalArg() {
        var intBlk1 = blockFactory.newIntArrayVector(new int[] { 10 }, 1).asBlock();
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer"));
        var pages = List.of(new Page(intBlk1));
        try (var response = new EsqlQueryResponse(columnInfo, pages, null, false, null, false, false)) {
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
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer"));
        var pages = List.of(new Page(blk1), new Page(blk2), new Page(blk3));
        try (var response = new EsqlQueryResponse(columnInfo, pages, null, false, null, false, false)) {
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
        var columnInfo = List.of(new ColumnInfoImpl("foo", "integer"));
        var pages = List.of(new Page(blk1), new Page(blk2), new Page(blk3));
        try (var response = new EsqlQueryResponse(columnInfo, pages, null, false, null, false, false)) {
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
            try (var resp = new EsqlQueryResponse(columns, pages, null, false, "", false, false)) {
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
                    case KEYWORD, TEXT, UNSUPPORTED -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(value.toString()));
                    case IP -> ((BytesRefBlock.Builder) builder).appendBytesRef(stringToIP(value.toString()));
                    case DATETIME -> {
                        long longVal = dateTimeToLong(value.toString());
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
                }
            }
        }
        return new Page(results.stream().map(Block.Builder::build).toArray(Block[]::new));
    }

}
