/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;

public record ColumnInfo(String name, String type) implements Writeable {

    private static final InstantiatingObjectParser<ColumnInfo, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ColumnInfo, Void> parser = InstantiatingObjectParser.builder(
            "esql/column_info",
            true,
            ColumnInfo.class
        );
        parser.declareString(constructorArg(), new ParseField("name"));
        parser.declareString(constructorArg(), new ParseField("type"));
        PARSER = parser.build();
    }

    public static ColumnInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ColumnInfo(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("type", type);
        builder.endObject();
        return builder;
    }

    public abstract class PositionToXContent {
        private final Block block;

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
    }

    public PositionToXContent positionToXContent(Block block, BytesRef scratch) {
        return switch (type) {
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
            case "keyword" -> new PositionToXContent(block) {
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
                    return builder.value(DocValueFormat.IP.format(val));
                }
            };
            case "date" -> new PositionToXContent(block) {
                @Override
                protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex)
                    throws IOException {
                    long longVal = ((LongBlock) block).getLong(valueIndex);
                    return builder.value(UTC_DATE_TIME_FORMATTER.formatMillis(longVal));
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
                    return builder.value(new Version(val).toString());
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
            default -> throw new IllegalArgumentException("can't convert values of type [" + type + "]");
        };
    }
}
