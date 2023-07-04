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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EsqlQueryResponseTests extends AbstractChunkedSerializingTestCase<EsqlQueryResponse> {
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
            Block.Builder builder = LocalExecutionPlanner.toElementType(EsqlDataTypes.fromEs(c.type())).newBlockBuilder(1);
            switch (c.type()) {
                case "unsigned_long", "long" -> ((LongBlock.Builder) builder).appendLong(randomLong());
                case "integer" -> ((IntBlock.Builder) builder).appendInt(randomInt());
                case "double" -> ((DoubleBlock.Builder) builder).appendDouble(randomDouble());
                case "keyword" -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
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
                yield new EsqlQueryResponse(cols, instance.pages(), instance.columnar());
            }
            case 1 -> new EsqlQueryResponse(instance.columns(), instance.pages(), false == instance.columnar());
            case 2 -> {
                int noPages = instance.pages().size();
                yield new EsqlQueryResponse(
                    instance.columns(),
                    randomValueOtherThan(instance.pages(), () -> randomList(noPages, noPages, () -> randomPage(instance.columns()))),
                    instance.columnar()
                );
            }
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected Writeable.Reader<EsqlQueryResponse> instanceReader() {
        return EsqlQueryResponse::new;
    }

    @Override
    protected EsqlQueryResponse doParseInstance(XContentParser parser) {
        return EsqlQueryResponse.fromXContent(parser);
    }

    public void testChunkResponseSizeColumnar() {
        EsqlQueryResponse resp = randomResponse(true);
        int columnCount = resp.pages().get(0).getBlockCount();
        int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount() * p.getBlockCount()).sum() + columnCount * 2;
        assertChunkCount(resp, r -> 5 + bodySize);
    }

    public void testChunkResponseSizeRows() {
        EsqlQueryResponse resp = randomResponse(false);
        int bodySize = resp.pages().stream().mapToInt(p -> p.getPositionCount()).sum();
        assertChunkCount(resp, r -> 5 + bodySize);
    }

    public void testSimpleXContentColumnar() {
        EsqlQueryResponse response = simple(true);
        assertThat(Strings.toString(response), equalTo("""
            {"columns":[{"name":"foo","type":"integer"}],"values":[[40,80]]}"""));
    }

    public void testSimpleXContentRows() {
        EsqlQueryResponse response = simple(false);
        assertThat(Strings.toString(response), equalTo("""
            {"columns":[{"name":"foo","type":"integer"}],"values":[[40],[80]]}"""));
    }

    private EsqlQueryResponse simple(boolean columnar) {
        return new EsqlQueryResponse(
            List.of(new ColumnInfo("foo", "integer")),
            List.of(new Page(new IntArrayVector(new int[] { 40, 80 }, 2).asBlock())),
            columnar
        );
    }
}
