/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.Objects;

public final class MatchConfig implements Writeable {
    private final String fieldName;
    private final int channel;
    private final DataType type;

    public MatchConfig(String fieldName, int channel, DataType type) {
        this.fieldName = fieldName;
        this.channel = channel;
        this.type = type;
    }

    public MatchConfig(String fieldName, Layout.ChannelAndType input) {
        this(fieldName, input.channel(), input.type());
    }

    public MatchConfig(StreamInput in) throws IOException {
        this(in.readString(), in.readInt(), DataType.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeInt(channel);
        type.writeTo(out);
    }

    public String fieldName() {
        return fieldName;
    }

    public int channel() {
        return channel;
    }

    public DataType type() {
        return type;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (MatchConfig) obj;
        return Objects.equals(this.fieldName, that.fieldName) && this.channel == that.channel && Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, channel, type);
    }

    @Override
    public String toString() {
        return "MatchConfig[" + "fieldName=" + fieldName + ", " + "channel=" + channel + ", " + "type=" + type + ']';
    }

}
