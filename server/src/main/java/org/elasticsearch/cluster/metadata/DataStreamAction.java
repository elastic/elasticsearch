/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Operations on data streams. Currently supports adding and removing backing indices.
 */
public class DataStreamAction implements Writeable, ToXContentObject {

    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField FAILURE_STORE = new ParseField("failure_store");

    private static final ParseField ADD_BACKING_INDEX = new ParseField("add_backing_index");
    private static final ParseField REMOVE_BACKING_INDEX = new ParseField("remove_backing_index");

    public enum Type {
        ADD_BACKING_INDEX((byte) 0, DataStreamAction.ADD_BACKING_INDEX),
        REMOVE_BACKING_INDEX((byte) 1, DataStreamAction.REMOVE_BACKING_INDEX);

        private final byte value;
        private final String fieldName;

        Type(byte value, ParseField field) {
            this.value = value;
            this.fieldName = field.getPreferredName();
        }

        public byte value() {
            return value;
        }

        public static Type fromValue(byte value) {
            return switch (value) {
                case 0 -> ADD_BACKING_INDEX;
                case 1 -> REMOVE_BACKING_INDEX;
                default -> throw new IllegalArgumentException("no data stream action type for [" + value + "]");
            };
        }
    }

    private final Type type;
    private String dataStream;
    private String index;
    private boolean failureStore = false;

    public static DataStreamAction addBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.ADD_BACKING_INDEX, dataStream, index, false);
    }

    public static DataStreamAction addFailureStoreIndex(String dataStream, String index) {
        return new DataStreamAction(Type.ADD_BACKING_INDEX, dataStream, index, true);
    }

    public static DataStreamAction removeBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.REMOVE_BACKING_INDEX, dataStream, index, false);
    }

    public static DataStreamAction removeFailureStoreIndex(String dataStream, String index) {
        return new DataStreamAction(Type.REMOVE_BACKING_INDEX, dataStream, index, true);
    }

    public DataStreamAction(StreamInput in) throws IOException {
        this.type = Type.fromValue(in.readByte());
        this.dataStream = in.readString();
        this.index = in.readString();
        this.failureStore = in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) && in.readBoolean();
    }

    private DataStreamAction(Type type, String dataStream, String index, boolean failureStore) {
        if (false == Strings.hasText(dataStream)) {
            throw new IllegalArgumentException("[data_stream] is required");
        }
        if (false == Strings.hasText(index)) {
            throw new IllegalArgumentException("[index] is required");
        }
        this.type = Objects.requireNonNull(type, "[type] must not be null");
        this.dataStream = dataStream;
        this.index = index;
        this.failureStore = failureStore;
    }

    DataStreamAction(Type type) {
        this.type = type;
    }

    public String getDataStream() {
        return dataStream;
    }

    public void setDataStream(String datastream) {
        this.dataStream = datastream;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public boolean isFailureStore() {
        return failureStore;
    }

    public void setFailureStore(boolean failureStore) {
        this.failureStore = failureStore;
    }

    public Type getType() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(type.fieldName);
        builder.field(DATA_STREAM.getPreferredName(), dataStream);
        builder.field(INDEX.getPreferredName(), index);
        if (failureStore) {
            builder.field(FAILURE_STORE.getPreferredName(), failureStore);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type.value());
        out.writeString(dataStream);
        out.writeString(index);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeBoolean(failureStore);
        }
    }

    public static DataStreamAction fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    private static final ObjectParser<DataStreamAction, Void> ADD_BACKING_INDEX_PARSER = parser(
        ADD_BACKING_INDEX.getPreferredName(),
        () -> new DataStreamAction(Type.ADD_BACKING_INDEX)
    );
    private static final ObjectParser<DataStreamAction, Void> REMOVE_BACKING_INDEX_PARSER = parser(
        REMOVE_BACKING_INDEX.getPreferredName(),
        () -> new DataStreamAction(Type.REMOVE_BACKING_INDEX)
    );
    static {
        ADD_BACKING_INDEX_PARSER.declareField(
            DataStreamAction::setDataStream,
            XContentParser::text,
            DATA_STREAM,
            ObjectParser.ValueType.STRING
        );
        ADD_BACKING_INDEX_PARSER.declareField(DataStreamAction::setIndex, XContentParser::text, INDEX, ObjectParser.ValueType.STRING);
        ADD_BACKING_INDEX_PARSER.declareField(
            DataStreamAction::setFailureStore,
            XContentParser::booleanValue,
            FAILURE_STORE,
            ObjectParser.ValueType.BOOLEAN
        );
        REMOVE_BACKING_INDEX_PARSER.declareField(
            DataStreamAction::setDataStream,
            XContentParser::text,
            DATA_STREAM,
            ObjectParser.ValueType.STRING
        );
        REMOVE_BACKING_INDEX_PARSER.declareField(DataStreamAction::setIndex, XContentParser::text, INDEX, ObjectParser.ValueType.STRING);
        REMOVE_BACKING_INDEX_PARSER.declareField(
            DataStreamAction::setFailureStore,
            XContentParser::booleanValue,
            FAILURE_STORE,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    private static ObjectParser<DataStreamAction, Void> parser(String name, Supplier<DataStreamAction> supplier) {
        ObjectParser<DataStreamAction, Void> parser = new ObjectParser<>(name, supplier);
        return parser;
    }

    public static final ConstructingObjectParser<DataStreamAction, Void> PARSER = new ConstructingObjectParser<>(
        "data_stream_action",
        a -> {
            // Take the first action and error if there is more than one action
            DataStreamAction action = null;
            for (Object o : a) {
                if (o != null) {
                    if (action == null) {
                        action = (DataStreamAction) o;
                    } else {
                        throw new IllegalArgumentException("too many data stream operations declared on operation entry");
                    }
                }
            }
            return action;
        }
    );
    static {
        PARSER.declareObject(optionalConstructorArg(), ADD_BACKING_INDEX_PARSER, ADD_BACKING_INDEX);
        PARSER.declareObject(optionalConstructorArg(), REMOVE_BACKING_INDEX_PARSER, REMOVE_BACKING_INDEX);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DataStreamAction other = (DataStreamAction) obj;
        return Objects.equals(type, other.type)
            && Objects.equals(dataStream, other.dataStream)
            && Objects.equals(index, other.index)
            && Objects.equals(failureStore, other.failureStore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, dataStream, index, failureStore);
    }
}
