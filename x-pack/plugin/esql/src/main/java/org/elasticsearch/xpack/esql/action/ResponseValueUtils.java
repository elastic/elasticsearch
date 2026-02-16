/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.aggregateMetricDoubleBlockToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateRangeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.exponentialHistogramBlockToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.geoGridToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.nanoTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.spatialToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

/**
 * Collection of static utility methods for helping transform response data between pages and values.
 */
public final class ResponseValueUtils {

    /**
     * Returns an iterator of iterators over the values in the given pages. There is one iterator
     * for each block.
     */
    public static Iterator<Iterator<Object>> pagesToValues(List<DataType> dataTypes, List<Page> pages, ZoneId zoneId) {
        BytesRef scratch = new BytesRef();
        var valueExtractors = valueExtractorsFor(dataTypes, zoneId);
        return Iterators.flatMap(
            pages.iterator(),
            page -> Iterators.forRange(
                0,
                page.getPositionCount(),
                pos -> Iterators.forRange(0, page.getBlockCount(), b -> valueAtPosition(valueExtractors[b], page.getBlock(b), pos, scratch))
            )
        );
    }

    /** Returns an iterable of iterables over the values in the given pages. There is one iterables for each row. */
    static Iterable<Iterable<Object>> valuesForRowsInPages(List<DataType> dataTypes, List<Page> pages, ZoneId zoneId) {
        BytesRef scratch = new BytesRef();
        var valueExtractors = valueExtractorsFor(dataTypes, zoneId);
        return () -> Iterators.flatMap(pages.iterator(), page -> valuesForRowsInPage(valueExtractors, page, scratch));
    }

    /** Returns an iterable of iterables over the values in the given page. There is one iterables for each row. */
    private static Iterator<Iterable<Object>> valuesForRowsInPage(BlockValueExtractor[] valueExtractors, Page page, BytesRef scratch) {
        return Iterators.forRange(0, page.getPositionCount(), position -> valuesForRow(valueExtractors, page, position, scratch));
    }

    /** Returns an iterable over the values in the given row in a page. */
    private static Iterable<Object> valuesForRow(BlockValueExtractor[] valueExtractors, Page page, int position, BytesRef scratch) {
        return () -> Iterators.forRange(
            0,
            page.getBlockCount(),
            blockIdx -> valueAtPosition(valueExtractors[blockIdx], page.getBlock(blockIdx), position, scratch)
        );
    }

    /**  Returns an iterator of values for the given column. */
    static Iterator<Object> valuesForColumn(int columnIndex, DataType dataType, List<Page> pages, ZoneId zoneId) {
        BytesRef scratch = new BytesRef();
        var valueExtractor = valueExtractorFor(dataType, zoneId);
        return Iterators.flatMap(
            pages.iterator(),
            page -> Iterators.forRange(
                0,
                page.getPositionCount(),
                pos -> valueAtPosition(valueExtractor, page.getBlock(columnIndex), pos, scratch)
            )
        );
    }

    /** Returns the value that the position and with the given data type, in the block. */
    private static Object valueAtPosition(BlockValueExtractor valueExtractor, Block block, int position, BytesRef scratch) {
        if (block.isNull(position)) {
            return null;
        }
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            return valueExtractor.extract(block, start, scratch);
        }
        List<Object> values = new ArrayList<>(count);
        int end = count + start;
        for (int i = start; i < end; i++) {
            values.add(valueExtractor.extract(block, i, scratch));
        }
        return values;
    }

    interface BlockValueExtractor {
        Object extract(Block block, int offset, BytesRef scratch);
    }

    private static BlockValueExtractor[] valueExtractorsFor(List<DataType> dataTypes, ZoneId zoneId) {
        var valueExtractors = new BlockValueExtractor[dataTypes.size()];
        for (int i = 0; i < dataTypes.size(); i++) {
            valueExtractors[i] = valueExtractorFor(dataTypes.get(i), zoneId);
        }
        return valueExtractors;
    }

    private static BlockValueExtractor valueExtractorFor(DataType dataType, ZoneId zoneId) {
        return switch (dataType) {
            case UNSIGNED_LONG -> (block, offset, scratch) -> unsignedLongAsNumber(((LongBlock) block).getLong(offset));
            case LONG, COUNTER_LONG -> (block, offset, scratch) -> ((LongBlock) block).getLong(offset);
            case INTEGER, COUNTER_INTEGER -> (block, offset, scratch) -> ((IntBlock) block).getInt(offset);
            case DOUBLE, COUNTER_DOUBLE -> (block, offset, scratch) -> ((DoubleBlock) block).getDouble(offset);
            case KEYWORD, TEXT -> (block, offset, scratch) -> ((BytesRefBlock) block).getBytesRef(offset, scratch).utf8ToString();
            case IP -> (block, offset, scratch) -> {
                BytesRef val = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                return ipToString(val);
            };
            case DATETIME -> {
                var formatter = DEFAULT_DATE_TIME_FORMATTER.withZone(zoneId);
                yield (block, offset, scratch) -> {
                    long longVal = ((LongBlock) block).getLong(offset);
                    return dateTimeToString(longVal, formatter);
                };
            }
            case DATE_NANOS -> {
                var formatter = DEFAULT_DATE_NANOS_FORMATTER.withZone(zoneId);
                yield (block, offset, scratch) -> {
                    long longVal = ((LongBlock) block).getLong(offset);
                    return nanoTimeToString(longVal, formatter);
                };
            }
            case BOOLEAN -> (block, offset, scratch) -> ((BooleanBlock) block).getBoolean(offset);
            case VERSION -> (block, offset, scratch) -> versionToString(((BytesRefBlock) block).getBytesRef(offset, scratch));
            case GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE -> (block, offset, scratch) -> spatialToString(
                ((BytesRefBlock) block).getBytesRef(offset, scratch)
            );
            case GEOHEX, GEOHASH, GEOTILE -> (block, offset, scratch) -> geoGridToString(((LongBlock) block).getLong(offset), dataType);
            case AGGREGATE_METRIC_DOUBLE -> (block, offset, scratch) -> aggregateMetricDoubleBlockToString(
                (AggregateMetricDoubleBlock) block,
                offset
            );
            case EXPONENTIAL_HISTOGRAM -> (block, offset, scratch) -> exponentialHistogramBlockToString(
                (ExponentialHistogramBlock) block,
                offset
            );
            case DATE_RANGE -> (block, offset, scratch) -> {
                var from = ((LongRangeBlock) block).getFromBlock().getLong(offset);
                var to = ((LongRangeBlock) block).getToBlock().getLong(offset);
                return dateRangeToString(from, to);
            };
            case TDIGEST -> (block, offset, scratch) -> ((TDigestBlock) block).getTDigestHolder(offset);
            case HISTOGRAM -> (block, offset, scratch) -> EsqlDataTypeConverter.histogramToString(
                ((BytesRefBlock) block).getBytesRef(offset, scratch)
            );
            case SOURCE -> (block, offset, scratch) -> {
                BytesRef val = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                try {
                    try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(val))) {
                        parser.nextToken();
                        return parser.mapOrdered();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            case TSID_DATA_TYPE -> (block, offset, scratch) -> {
                BytesRef val = ((BytesRefBlock) block).getBytesRef(offset, scratch);
                return TimeSeriesIdFieldMapper.encodeTsid(val);
            };
            case DENSE_VECTOR -> (block, offset, scratch) -> ((FloatBlock) block).getFloat(offset);
            case NULL, UNSUPPORTED -> (block, offset, scratch) -> null;
            case SHORT, BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT, OBJECT, DATE_PERIOD, TIME_DURATION, DOC_DATA_TYPE ->
                throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }
}
