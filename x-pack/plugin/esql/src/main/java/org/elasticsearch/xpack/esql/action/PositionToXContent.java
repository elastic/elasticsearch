/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.aggregateMetricDoubleBlockToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.nanoTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.spatialToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

abstract class PositionToXContent {
    protected final Block block;

    PositionToXContent(Block block) {
        this.block = block;
    }

    public XContentBuilder positionToXContent(XContentBuilder builder, ToXContent.Params params, int position) throws IOException {
        if (block.isNull(position)) {
            return builder.nullValue();
        }
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            return valueToXContent(builder, params, start);
        }
        builder.startArray();
        int end = start + count;
        for (int i = start; i < end; i++) {
            valueToXContent(builder, params, i);
        }
        return builder.endArray();
    }

    protected abstract XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
        throws IOException;

    public static PositionToXContent positionToXContent(ColumnInfoImpl columnInfo, Block block, BytesRef scratch) {
        return switch (columnInfo.type()) {
            case LONG, COUNTER_LONG -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((LongBlock) block).getLong(valueIndex));
                }
            };
            case INTEGER, COUNTER_INTEGER -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((IntBlock) block).getInt(valueIndex));
                }
            };
            case DOUBLE, COUNTER_DOUBLE -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((DoubleBlock) block).getDouble(valueIndex));
                }
            };
            case UNSIGNED_LONG -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    long l = ((LongBlock) block).getLong(valueIndex);
                    return builder.value(unsignedLongAsNumber(l));
                }
            };
            case KEYWORD, SEMANTIC_TEXT, TEXT -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                    if (builder.contentType() == XContentType.CBOR && val.offset != 0) {
                        // cbor needs a zero offset because of a bug in jackson
                        // https://github.com/FasterXML/jackson-dataformats-binary/issues/366
                        val = BytesRef.deepCopyOf(scratch);
                    }
                    return builder.utf8Value(val.bytes, val.offset, val.length);
                }
            };
            case IP -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                    return builder.value(ipToString(val));
                }
            };
            case DATETIME -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    long longVal = ((LongBlock) block).getLong(valueIndex);
                    return builder.value(dateTimeToString(longVal));
                }
            };
            case DATE_NANOS -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    long longVal = ((LongBlock) block).getLong(valueIndex);
                    return builder.value(nanoTimeToString(longVal));
                }
            };
            case GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(spatialToString(((BytesRefBlock) block).getBytesRef(valueIndex, scratch)));
                }
            };
            case BOOLEAN -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((BooleanBlock) block).getBoolean(valueIndex));
                }
            };
            case VERSION -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                    return builder.value(versionToString(val));
                }
            };
            case AGGREGATE_METRIC_DOUBLE -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(aggregateMetricDoubleBlockToString((AggregateMetricDoubleBlock) block, valueIndex));
                }
            };
            case NULL -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.nullValue();
                }
            };
            case UNSUPPORTED -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value((String) null);
                }
            };
            case SOURCE -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                    try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(val))) {
                        parser.nextToken();
                        return builder.copyCurrentStructure(parser);
                    }
                }
            };
            case DATE_PERIOD, TIME_DURATION, DOC_DATA_TYPE, TSID_DATA_TYPE, SHORT, BYTE, OBJECT, FLOAT, HALF_FLOAT, SCALED_FLOAT,
                PARTIAL_AGG -> throw new IllegalArgumentException("can't convert values of type [" + columnInfo.type() + "]");
        };
    }
}
