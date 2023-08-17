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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.ql.util.StringUtils.parseIP;

public class EsqlQueryResponse extends ActionResponse implements ChunkedToXContent {

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

    public EsqlQueryResponse(StreamInput in) throws IOException {
        super(in);
        this.columns = in.readList(ColumnInfo::new);
        this.pages = in.readList(Page::new);
        this.columnar = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(columns);
        out.writeList(pages);
        out.writeBoolean(columnar);
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    List<Page> pages() {
        return pages;
    }

    public List<List<Object>> values() {
        return pagesToValues(columns.stream().map(ColumnInfo::type).toList(), pages);
    }

    public boolean columnar() {
        return columnar;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params unused) {
        BytesRef scratch = new BytesRef();
        final Iterator<ToXContent> valuesIt;
        if (pages.isEmpty()) {
            valuesIt = Collections.emptyIterator();
        } else if (columnar) {
            valuesIt = IntStream.range(0, columns().size()).mapToObj(column -> {
                Stream<ToXContent> values = pages.stream().flatMap(page -> {
                    ColumnInfo.PositionToXContent toXContent = columns.get(column).positionToXContent(page.getBlock(column), scratch);
                    return IntStream.range(0, page.getPositionCount())
                        .mapToObj(position -> (builder, params) -> toXContent.positionToXContent(builder, params, position));
                });
                return Stream.concat(
                    Stream.of((builder, params) -> builder.startArray()),
                    Stream.concat(values, Stream.of((builder, params) -> builder.endArray()))
                );
            }).flatMap(Function.identity()).iterator();
        } else {
            valuesIt = pages.stream().flatMap(page -> {
                List<ColumnInfo.PositionToXContent> toXContents = IntStream.range(0, page.getBlockCount())
                    .mapToObj(column -> columns.get(column).positionToXContent(page.getBlock(column), scratch))
                    .toList();
                return IntStream.range(0, page.getPositionCount()).mapToObj(position -> (ToXContent) (builder, params) -> {
                    builder.startArray();
                    for (int c = 0; c < columns.size(); c++) {
                        toXContents.get(c).positionToXContent(builder, params, position);
                    }
                    return builder.endArray();
                });
            }).iterator();
        }
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(), //
            ChunkedToXContentHelper.singleChunk((builder, params) -> {
                builder.startArray("columns");
                for (ColumnInfo col : columns) {
                    col.toXContent(builder, params);
                }
                builder.endArray();
                return builder;
            }),//
            ChunkedToXContentHelper.array("values", valuesIt),//
            ChunkedToXContentHelper.endObject()
        );
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
        return Objects.equals(columns, that.columns) && Objects.equals(values(), that.values()) && columnar == that.columnar;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, values(), columnar);
    }

    @Override
    public String toString() {
        return Strings.toString(ChunkedToXContent.wrapAsToXContent(this));
    }

    public static List<List<Object>> pagesToValues(List<String> dataTypes, List<Page> pages) {
        BytesRef scratch = new BytesRef();
        List<List<Object>> result = new ArrayList<>();
        for (Page page : pages) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> row = new ArrayList<>(page.getBlockCount());
                for (int b = 0; b < page.getBlockCount(); b++) {
                    Block block = page.getBlock(b);
                    if (block.isNull(p)) {
                        row.add(null);
                        continue;
                    }
                    /*
                     * Use the ESQL data type to map to the output to make sure compute engine
                     * respects its types. See the INTEGER clause where is doesn't always
                     * respect it.
                     */
                    int count = block.getValueCount(p);
                    int start = block.getFirstValueIndex(p);
                    if (count == 1) {
                        row.add(valueAt(dataTypes.get(b), block, start, scratch));
                        continue;
                    }
                    List<Object> thisResult = new ArrayList<>(count);
                    int end = count + start;
                    for (int i = start; i < end; i++) {
                        thisResult.add(valueAt(dataTypes.get(b), block, i, scratch));
                    }
                    row.add(thisResult);
                }
                result.add(row);
            }
        }
        return result;
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
            default -> throw new UnsupportedOperationException("unsupported data type [" + dataType + "]");
        };
    }

    /**
     * Convert a list of values to Pages so we can parse from xcontent. It's not
     * super efficient but it doesn't really have to be.
     */
    private static Page valuesToPage(List<String> dataTypes, List<List<Object>> values) {
        List<Block.Builder> results = dataTypes.stream()
            .map(c -> LocalExecutionPlanner.toElementType(EsqlDataTypes.fromEs(c)).newBlockBuilder(values.size()))
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
                    default -> throw new UnsupportedOperationException("unsupported data type [" + dataTypes.get(c) + "]");
                }
            }
        }
        return new Page(results.stream().map(Block.Builder::build).toArray(Block[]::new));
    }
}
