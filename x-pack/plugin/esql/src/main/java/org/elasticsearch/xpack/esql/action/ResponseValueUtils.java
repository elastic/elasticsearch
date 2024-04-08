/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.spatialToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIP;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToSpatial;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToVersion;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

/**
 * Collection of static utility methods for helping transform response data between pages and values.
 */
public final class ResponseValueUtils {

    /**
     * Returns an iterator of iterators over the values in the given pages. There is one iterator
     * for each block.
     */
    public static Iterator<Iterator<Object>> pagesToValues(List<String> dataTypes, List<Page> pages) {
        BytesRef scratch = new BytesRef();
        return Iterators.flatMap(
            pages.iterator(),
            page -> Iterators.forRange(
                0,
                page.getPositionCount(),
                pos -> Iterators.forRange(0, page.getBlockCount(), b -> valueAtPosition(page.getBlock(b), pos, dataTypes.get(b), scratch))
            )
        );
    }

    /** Returns an iterable of iterables over the values in the given pages. There is one iterables for each row. */
    static Iterable<Iterable<Object>> valuesForRowsInPages(List<String> dataTypes, List<Page> pages) {
        BytesRef scratch = new BytesRef();
        return () -> Iterators.flatMap(pages.iterator(), page -> valuesForRowsInPage(dataTypes, page, scratch));
    }

    /** Returns an iterable of iterables over the values in the given page. There is one iterables for each row. */
    static Iterator<Iterable<Object>> valuesForRowsInPage(List<String> dataTypes, Page page, BytesRef scratch) {
        return Iterators.forRange(0, page.getPositionCount(), position -> valuesForRow(dataTypes, page, position, scratch));
    }

    /** Returns an iterable over the values in the given row in a page. */
    static Iterable<Object> valuesForRow(List<String> dataTypes, Page page, int position, BytesRef scratch) {
        return () -> Iterators.forRange(
            0,
            page.getBlockCount(),
            blockIdx -> valueAtPosition(page.getBlock(blockIdx), position, dataTypes.get(blockIdx), scratch)
        );
    }

    /**  Returns an iterator of values for the given column. */
    static Iterator<Object> valuesForColumn(int columnIndex, String dataType, List<Page> pages) {
        BytesRef scratch = new BytesRef();
        return Iterators.flatMap(
            pages.iterator(),
            page -> Iterators.forRange(
                0,
                page.getPositionCount(),
                pos -> valueAtPosition(page.getBlock(columnIndex), pos, dataType, scratch)
            )
        );
    }

    /** Returns the value that the position and with the given data type, in the block. */
    static Object valueAtPosition(Block block, int position, String dataType, BytesRef scratch) {
        if (block.isNull(position)) {
            return null;
        }
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            return valueAt(dataType, block, start, scratch);
        }
        List<Object> values = new ArrayList<>(count);
        int end = count + start;
        for (int i = start; i < end; i++) {
            values.add(valueAt(dataType, block, i, scratch));
        }
        return values;
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
                yield ipToString(val);
            }
            case "date" -> {
                long longVal = ((LongBlock) block).getLong(offset);
                yield dateTimeToString(longVal);
            }
            case "boolean" -> ((BooleanBlock) block).getBoolean(offset);
            case "version" -> versionToString(((BytesRefBlock) block).getBytesRef(offset, scratch));
            case "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" -> spatialToString(
                ((BytesRefBlock) block).getBytesRef(offset, scratch)
            );
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
     * Converts a list of values to Pages so that we can parse from xcontent. It's not
     * super efficient, but it doesn't really have to be.
     */
    static Page valuesToPage(BlockFactory blockFactory, List<ColumnInfo> columns, List<List<Object>> values) {
        List<String> dataTypes = columns.stream().map(ColumnInfo::type).toList();
        List<Block.Builder> results = dataTypes.stream()
            .map(c -> PlannerUtils.toElementType(EsqlDataTypes.fromName(c)).newBlockBuilder(values.size(), blockFactory))
            .toList();

        for (List<Object> row : values) {
            for (int c = 0; c < row.size(); c++) {
                var builder = results.get(c);
                var value = row.get(c);
                switch (dataTypes.get(c)) {
                    case "unsigned_long" -> ((LongBlock.Builder) builder).appendLong(
                        longToUnsignedLong(((Number) value).longValue(), true)
                    );
                    case "long" -> ((LongBlock.Builder) builder).appendLong(((Number) value).longValue());
                    case "integer" -> ((IntBlock.Builder) builder).appendInt(((Number) value).intValue());
                    case "double" -> ((DoubleBlock.Builder) builder).appendDouble(((Number) value).doubleValue());
                    case "keyword", "text", "unsupported" -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                        new BytesRef(value.toString())
                    );
                    case "ip" -> ((BytesRefBlock.Builder) builder).appendBytesRef(stringToIP(value.toString()));
                    case "date" -> {
                        long longVal = dateTimeToLong(value.toString());
                        ((LongBlock.Builder) builder).appendLong(longVal);
                    }
                    case "boolean" -> ((BooleanBlock.Builder) builder).appendBoolean(((Boolean) value));
                    case "null" -> builder.appendNull();
                    case "version" -> ((BytesRefBlock.Builder) builder).appendBytesRef(stringToVersion(new BytesRef(value.toString())));
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
                    case "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" -> {
                        // This just converts WKT to WKB, so does not need CRS knowledge, we could merge GEO and CARTESIAN here
                        BytesRef wkb = stringToSpatial(value.toString());
                        ((BytesRefBlock.Builder) builder).appendBytesRef(wkb);
                    }
                    default -> throw EsqlIllegalArgumentException.illegalDataType(dataTypes.get(c));
                }
            }
        }
        return new Page(results.stream().map(Block.Builder::build).toArray(Block[]::new));
    }
}
