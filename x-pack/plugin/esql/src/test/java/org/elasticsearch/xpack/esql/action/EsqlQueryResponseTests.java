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
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
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
        return new NamedWriteableRegistry(Block.getNamedWriteables());
    }

    @Override
    protected EsqlQueryResponse createXContextTestInstance(XContentType xContentType) {
        // columnar param can't be different from the default value (false) since the EsqlQueryResponse will be serialized (by some random
        // XContentType, not to a StreamOutput) and parsed back, which doesn't preserve columnar field's value.
        return randomResponse(false);
    }

    @Override
    protected EsqlQueryResponse createTestInstance() {
        return randomResponse(randomBoolean());
    }

    EsqlQueryResponse randomResponse(boolean columnar) {
        int noCols = randomIntBetween(1, 10);
        List<ColumnInfo> columns = randomList(noCols, noCols, this::randomColumnInfo);
        int noPages = randomIntBetween(1, 20);
        List<Page> values = randomList(noPages, noPages, () -> randomPage(columns));
        return new EsqlQueryResponse(columns, values, columnar);
    }

    private ColumnInfo randomColumnInfo() {
        DataType type = randomValueOtherThanMany(
            t -> false == DataTypes.isPrimitive(t) || t == EsqlDataTypes.DATE_PERIOD || t == EsqlDataTypes.TIME_DURATION,
            () -> randomFrom(EsqlDataTypes.types())
        );
        type = EsqlDataTypes.widenSmallNumericTypes(type);
        return new ColumnInfo(randomAlphaOfLength(10), type.esType());
    }

    private Page randomPage(List<ColumnInfo> columns) {
        return new Page(columns.stream().map(c -> {
            Block.Builder builder = LocalExecutionPlanner.toElementType(EsqlDataTypes.fromName(c.type())).newBlockBuilder(1, blockFactory);
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
        return switch (allNull ? between(0, 1) : between(0, 2)) {
            case 0 -> {
                int mutCol = between(0, instance.columns().size() - 1);
                List<ColumnInfo> cols = new ArrayList<>(instance.columns());
                // keep the type the same so the values are still valid but change the name
                cols.set(mutCol, new ColumnInfo(cols.get(mutCol).name() + "mut", cols.get(mutCol).type()));
                yield new EsqlQueryResponse(cols, deepCopyOfPages(instance), instance.columnar());
            }
            case 1 -> new EsqlQueryResponse(instance.columns(), deepCopyOfPages(instance), false == instance.columnar());
            case 2 -> {
                int noPages = instance.pages().size();
                List<Page> differentPages = List.of();
                do {
                    differentPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
                    differentPages = randomList(noPages, noPages, () -> randomPage(instance.columns()));
                } while (differentPages.equals(instance.pages()));
                yield new EsqlQueryResponse(instance.columns(), differentPages, instance.columnar());
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
        return EsqlQueryResponse.fromXContent(parser);
    }

    public void testChunkResponseSizeColumnar() {
        try (EsqlQueryResponse resp = randomResponse(true)) {
            int columnCount = resp.pages().get(0).getBlockCount();
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount() * p.getBlockCount()).sum() + columnCount * 2;
            assertChunkCount(resp, r -> 5 + bodySize);
        }
    }

    public void testChunkResponseSizeRows() {
        try (EsqlQueryResponse resp = randomResponse(false)) {
            int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount()).sum();
            assertChunkCount(resp, r -> 5 + bodySize);
        }
    }

    public void testSimpleXContentColumnar() {
        try (EsqlQueryResponse response = simple(true)) {
            assertThat(Strings.toString(response), equalTo("""
                {"columns":[{"name":"foo","type":"integer"}],"values":[[40,80]]}"""));
        }
    }

    public void testSimpleXContentRows() {
        try (EsqlQueryResponse response = simple(false)) {
            assertThat(Strings.toString(response), equalTo("""
                {"columns":[{"name":"foo","type":"integer"}],"values":[[40],[80]]}"""));
        }
    }

    private EsqlQueryResponse simple(boolean columnar) {
        return new EsqlQueryResponse(
            List.of(new ColumnInfo("foo", "integer")),
            List.of(new Page(new IntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
            columnar
        );
    }

    @Override
    protected void dispose(EsqlQueryResponse esqlQueryResponse) {
        esqlQueryResponse.close();
    }
}
