/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.ql.util.StringUtils.parseIP;

public class EsqlQueryResponse extends ActionResponse implements ChunkedToXContent, Releasable {

    private final List<ColumnInfo> columns;
    private final List<Page> pages;
    private final boolean columnar;

    private static final InstantiatingObjectParser<EsqlQueryResponse, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<EsqlQueryResponse, Void> parser = InstantiatingObjectParser.builder(
            "esql/query_response",
            true,
            EsqlQueryResponse.class
        );
        parser.declareObjectArray(constructorArg(), (p, c) -> ColumnInfo.fromXContent(p), new ParseField("columns"));
        parser.declareField(constructorArg(), (p, c) -> p.list(), new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER = parser.build();
    }

    public EsqlQueryResponse(List<ColumnInfo> columns, List<Page> pages, boolean columnar) {
        this.columns = columns;
        this.pages = pages;
        this.columnar = columnar;
    }

    public EsqlQueryResponse(List<ColumnInfo> columns, List<List<Object>> values) {
        this.columns = columns;
        this.pages = List.of(valuesToPage(columns.stream().map(ColumnInfo::type).toList(), values));
        this.columnar = false;
    }

    /**
     * Build a reader for the response.
     */
    public static Writeable.Reader<EsqlQueryResponse> reader(BlockFactory blockFactory) {
        return in -> new EsqlQueryResponse(new BlockStreamInput(in, blockFactory));
    }

    public EsqlQueryResponse(BlockStreamInput in) throws IOException {
        super(in);
        this.columns = in.readCollectionAsList(ColumnInfo::new);
        this.pages = in.readCollectionAsList(Page::new);
        this.columnar = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(columns);
        out.writeCollection(pages);
        out.writeBoolean(columnar);
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    List<Page> pages() {
        return pages;
    }

    public Iterator<Iterator<Object>> values() {
        return pagesToValues(columns.stream().map(ColumnInfo::type).toList(), pages);
    }

    public boolean columnar() {
        return columnar;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params unused) {
        final BytesRef scratch = new BytesRef();
        final Iterator<? extends ToXContent> valuesIt;
        if (pages.isEmpty()) {
            valuesIt = Collections.emptyIterator();
        } else if (columnar) {
            valuesIt = Iterators.flatMap(
                Iterators.forRange(
                    0,
                    columns().size(),
                    column -> Iterators.concat(
                        Iterators.single(((builder, params) -> builder.startArray())),
                        Iterators.flatMap(pages.iterator(), page -> {
                            ColumnInfo.PositionToXContent toXContent = columns.get(column)
                                .positionToXContent(page.getBlock(column), scratch);
                            return Iterators.forRange(
                                0,
                                page.getPositionCount(),
                                position -> (builder, params) -> toXContent.positionToXContent(builder, params, position)
                            );
                        }),
                        ChunkedToXContentHelper.endArray()
                    )
                ),
                Function.identity()
            );
        } else {
            valuesIt = Iterators.flatMap(pages.iterator(), page -> {
                final int columnCount = columns.size();
                assert page.getBlockCount() == columnCount : page.getBlockCount() + " != " + columnCount;
                final ColumnInfo.PositionToXContent[] toXContents = new ColumnInfo.PositionToXContent[columnCount];
                for (int column = 0; column < columnCount; column++) {
                    toXContents[column] = columns.get(column).positionToXContent(page.getBlock(column), scratch);
                }
                return Iterators.forRange(0, page.getPositionCount(), position -> (builder, params) -> {
                    builder.startArray();
                    for (int c = 0; c < columnCount; c++) {
                        toXContents[c].positionToXContent(builder, params, position);
                    }
                    return builder.endArray();
                });
            });
        }
        return Iterators.concat(ChunkedToXContentHelper.startObject(), ChunkedToXContentHelper.singleChunk((builder, params) -> {
            builder.startArray("columns");
            for (ColumnInfo col : columns) {
                col.toXContent(builder, params);
            }
            return builder.endArray();
        }), ChunkedToXContentHelper.array("values", valuesIt), ChunkedToXContentHelper.endObject());
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public static EsqlQueryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlQueryResponse that = (EsqlQueryResponse) o;
        return Objects.equals(columns, that.columns)
            && columnar == that.columnar
            && Iterators.equals(values(), that.values(), (row1, row2) -> Iterators.equals(row1, row2, Objects::equals));
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, Iterators.hashCode(values(), row -> Iterators.hashCode(row, Objects::hashCode)), columnar);
    }

    @Override
    public String toString() {
        return Strings.toString(ChunkedToXContent.wrapAsToXContent(this));
    }

    @Override
    public void close() {
        Releasables.close(() -> Iterators.map(pages.iterator(), p -> p::releaseBlocks));
    }

    public static Iterator<Iterator<Object>> pagesToValues(List<String> dataTypes, List<Page> pages) {
        BytesRef scratch = new BytesRef();
        return Iterators.flatMap(
            pages.iterator(),
            page -> Iterators.forRange(0, page.getPositionCount(), p -> Iterators.forRange(0, page.getBlockCount(), b -> {
                Block block = page.getBlock(b);
                if (block.isNull(p)) {
                    return null;
                }
                /*
                 * Use the ESQL data type to map to the output to make sure compute engine
                 * respects its types. See the INTEGER clause where is doesn't always
                 * respect it.
                 */
                int count = block.getValueCount(p);
                int start = block.getFirstValueIndex(p);
                String dataType = dataTypes.get(b);
                if (count == 1) {
                    return valueAt(dataType, block, start, scratch);
                }
                List<Object> thisResult = new ArrayList<>(count);
                int end = count + start;
                for (int i = start; i < end; i++) {
                    thisResult.add(valueAt(dataType, block, i, scratch));
                }
                return thisResult;
            }))
        );
    }

    private static Object valueAt(String dataType, Block block, int offset, BytesRef scratch) {
        return switch (dataType) {
            case "unsigned_long" -> unsignedLongAsNumber(((LongBlock) block).getLong(offset));
            case "long" -> ((LongBlock) block).getLong(offset);
            case "integer" -> ((IntBlock) block).getInt(offset);
            case "double" -> ((DoubleBlock) block).getDouble(offset);
            case "keyword", "text" -> ((BytesRefBlock) block).getBytesRef(offset, scratch).utf8ToString();
            case "ip" -> {
                BytesRef val = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                yield DocValueFormat.IP.format(val);
            }
            case "date" -> {
                long longVal = ((LongBlock) block).getLong(offset);
                yield UTC_DATE_TIME_FORMATTER.formatMillis(longVal);
            }
            case "boolean" -> ((BooleanBlock) block).getBoolean(offset);
            case "version" -> new Version(((BytesRefBlock) block).getBytesRef(offset, scratch)).toString();
            case "unsupported" -> UnsupportedValueSource.UNSUPPORTED_OUTPUT;
            case "_source" -> {
                BytesRef val = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                try {
                    try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(val))) {
                        parser.nextToken();
                        yield parser.mapOrdered();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    /**
     * Convert a list of values to Pages so we can parse from xcontent. It's not
     * super efficient but it doesn't really have to be.
     */
    private static Page valuesToPage(List<String> dataTypes, List<List<Object>> values) {
        List<Block.Builder> results = dataTypes.stream()
            .map(c -> LocalExecutionPlanner.toElementType(EsqlDataTypes.fromName(c)).newBlockBuilder(values.size()))
            .toList();

        for (List<Object> row : values) {
            for (int c = 0; c < row.size(); c++) {
                var builder = results.get(c);
                var value = row.get(c);
                switch (dataTypes.get(c)) {
                    case "unsigned_long" -> ((LongBlock.Builder) builder).appendLong(asLongUnsigned(((Number) value).longValue()));
                    case "long" -> ((LongBlock.Builder) builder).appendLong(((Number) value).longValue());
                    case "integer" -> ((IntBlock.Builder) builder).appendInt(((Number) value).intValue());
                    case "double" -> ((DoubleBlock.Builder) builder).appendDouble(((Number) value).doubleValue());
                    case "keyword", "text", "unsupported" -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                        new BytesRef(value.toString())
                    );
                    case "ip" -> ((BytesRefBlock.Builder) builder).appendBytesRef(parseIP(value.toString()));
                    case "date" -> {
                        long longVal = UTC_DATE_TIME_FORMATTER.parseMillis(value.toString());
                        ((LongBlock.Builder) builder).appendLong(longVal);
                    }
                    case "boolean" -> ((BooleanBlock.Builder) builder).appendBoolean(((Boolean) value));
                    case "null" -> builder.appendNull();
                    case "version" -> ((BytesRefBlock.Builder) builder).appendBytesRef(new Version(value.toString()).toBytesRef());
                    case "_source" -> {
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
                    default -> throw EsqlIllegalArgumentException.illegalDataType(dataTypes.get(c));
                }
            }
        }
        return new Page(results.stream().map(Block.Builder::build).toArray(Block[]::new));
    }
}
