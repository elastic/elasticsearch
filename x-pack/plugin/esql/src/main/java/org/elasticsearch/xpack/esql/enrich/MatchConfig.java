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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.ESQL_LOOKUP_JOIN_GENERAL_EXPRESSION;

/**
 * Configuration for a field used in the join condition of a LOOKUP JOIN or ENRICH operation.
 * <p>
 * This class specifies how to match a field from the input data (the "left" side of the join)
 * with a field in the lookup index (the "right" side). The interpretation of its properties
 * depends on the type of join.
 * <p>
 * For simple field-based joins (e.g., {@code ... ON field1, field2}), this configuration
 * represents the right-side field ({@code right.field}). In this case, {@link #fieldName} is the
 * name of the field in the lookup index used to build the query.
 * <p>
 * For expression-based joins (e.g., {@code ... ON left_field > right_field}), this
 * configuration represents the left-side field ({@code left_field}). In this case,
 * {@link #fieldName} is the name of the field whose value is sent to the lookup node.
 * <p>
 * The {@link #channel} identifies the position of this field's values within the internal
 * page sent to the lookup node.
 */
public final class MatchConfig implements Writeable {
    private final NamedExpression fieldName;
    private final int channel;
    private final DataType type;

    public MatchConfig(NamedExpression fieldName, int channel, DataType type) {
        this.fieldName = fieldName;
        this.channel = channel;
        this.type = type;
    }

    public MatchConfig(NamedExpression fieldName, Layout.ChannelAndType input) {
        this(fieldName, input.channel(), input.type());
    }

    public MatchConfig(StreamInput in) throws IOException {
        PlanStreamInput planIn = (PlanStreamInput) in;
        if (in.getTransportVersion().onOrAfter(ESQL_LOOKUP_JOIN_GENERAL_EXPRESSION)) {
            this.fieldName = planIn.readNamedWriteable(NamedExpression.class);
            this.channel = in.readInt();
            this.type = DataType.readFrom(in);
        } else {
            // Old format: fieldName (string), channel (int), type (DataType)
            String fieldNameString = in.readString();
            this.channel = in.readInt();
            this.type = DataType.readFrom(in);
            this.fieldName = new FieldAttribute(
                Source.EMPTY,
                null,
                null,
                fieldNameString,
                new EsField(fieldNameString, this.type, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            );
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(ESQL_LOOKUP_JOIN_GENERAL_EXPRESSION)) {
            // New format: write NamedExpression directly, same as Enrich and EnrichExec
            out.writeNamedWriteable(fieldName);
        } else {
            // Old format: write field name as string
            out.writeString(fieldName.name());
        }
        out.writeInt(channel);
        type.writeTo(out);
    }

    public NamedExpression fieldName() {
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
