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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.spatialToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

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

    public static PositionToXContent positionToXContent(ColumnInfo columnInfo, Block block, BytesRef scratch) {
        return switch (columnInfo.type()) {
            case "long" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((LongBlock) block).getLong(valueIndex));
                }
            };
            case "integer" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((IntBlock) block).getInt(valueIndex));
                }
            };
            case "double" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((DoubleBlock) block).getDouble(valueIndex));
                }
            };
            case "unsigned_long" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    long l = ((LongBlock) block).getLong(valueIndex);
                    return builder.value(unsignedLongAsNumber(l));
                }
            };
            case "keyword", "text" -> new PositionToXContent(block) {
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
            case "ip" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                    return builder.value(ipToString(val));
                }
            };
            case "date" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    long longVal = ((LongBlock) block).getLong(valueIndex);
                    return builder.value(dateTimeToString(longVal));
                }
            };
            case "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(spatialToString(((BytesRefBlock) block).getBytesRef(valueIndex, scratch)));
                }
            };
            case "boolean" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(((BooleanBlock) block).getBoolean(valueIndex));
                }
            };
            case "version" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                    return builder.value(versionToString(val));
                }
            };
            case "null" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.nullValue();
                }
            };
            case "unsupported" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    return builder.value(UnsupportedValueSource.UNSUPPORTED_OUTPUT);
                }
            };
            case "_source" -> new PositionToXContent(block) {
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
            default -> throw new IllegalArgumentException("can't convert values of type [" + columnInfo.type() + "]");
        };
    }
}
