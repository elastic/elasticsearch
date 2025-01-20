/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * A special marker for fields explicitly marked as (potentially) unmapped in the query. These need to be explicitly marked, so we know to
 * search for during data loading. By default, the {@link DataType} of these fields is {@link DataType#KEYWORD}, since we can always convert
 * a {@link DataType#KEYWORD} to any other type. However, this can cause type conflicts with partially mapped types, i.e, types which appear
 * in some indices but not all. In that sense, this field is very similar to union types, except we allow for unmapped fields. In fact, a
 * partially unmapped field can be treated as a special case of union type, however its resolution implementation might be different.

 * Check out the {@link State} algebraic data type for the different kinds of unmapped fields.
 */
public class PotentiallyUnmappedEsField extends EsField {
    private PotentiallyUnmappedEsField(State state, String name, DataType dataType, Map<String, EsField> properties) {
        super(name, dataType, properties, true /* aggregatable */);
        this.state = state;
    }

    public sealed interface State {}

    /**
     * A field which is either unmapped in all indices, or mapped to {@link DataType#KEYWORD} in all indices where it is mapped.
     * In either case, there is no type conflict.
     */
    public enum KeywordResolved implements State {
        INSTANCE;
    }

    /**
     * A field which is mapped to a single type, which is not {@link DataType#KEYWORD}. This can be resolved using a cast, similar to
     * union types. This is treated differently from {@link Invalid}, since resolving this conflict doesn't use
     * {@link MultiTypeEsField}.
     */
    public record SimpleConflict(DataType otherType) implements State {
        public SimpleConflict {
            if (otherType == KEYWORD) {
                throw new IllegalArgumentException("Use NoConflicts.INSTANCE for KEYWORD");
            }
        }
    }

    /**
     * A field which is mapped to a single type, but that type is not {@link DataType#KEYWORD}.
     * This is resolved using the specified conversions.
     */
    public record SimpleResolution(Expression unmappedConversion, Expression mappedConversion) implements State {}

    /**
     * A field which is mapped to more than one type in multiple indices, *in addition* to the unmapped case which is always treated as
     * {@link DataType#KEYWORD}. This can be resolved using a cast, similar to union types.
     */
    public record Invalid(InvalidMappedField invalidMappedField) implements State {}

    /**
     * A field which is mapped to different types in different indices, but resolved using union types. In mapped indices, we treat this
     * as a union type, and use the specified conversion for unmapped indices.
     */
    public record MultiType(Expression conversionFromKeyword, MultiTypeEsField multiTypeEsField) implements State {}

    private final State state;

    public State getState() {
        return state;
    }

    public static PotentiallyUnmappedEsField fromField(EsField f) {
        State state = switch (f) {
            case InvalidMappedField imf -> {
                var newTypesToIndices = new TreeMap<>(imf.getTypesToIndices());
                newTypesToIndices.compute(KEYWORD.typeName(), (k, v) -> v == null ? new TreeSet<>() : new TreeSet<>(v))
                    .add("unmapped field");
                yield new Invalid(imf.withTypesToIndices(newTypesToIndices));
            }
            case MultiTypeEsField mf -> throw new IllegalArgumentException("Use fromMultiType for MultiTypeEsField");
            default -> f.getDataType() == KEYWORD ? KeywordResolved.INSTANCE : new SimpleConflict(f.getDataType());
        };
        return new PotentiallyUnmappedEsField(state, f.getName(), f.getDataType(), f.getProperties());
    }

    public static PotentiallyUnmappedEsField fromStandalone(String name) {
        return new PotentiallyUnmappedEsField(KeywordResolved.INSTANCE, name, KEYWORD, Map.of());
    }

    public static PotentiallyUnmappedEsField fromMultiType(Expression expression, MultiTypeEsField multiTypeEsField) {
        return new PotentiallyUnmappedEsField(
            new MultiType(expression, multiTypeEsField),
            multiTypeEsField.getName(),
            multiTypeEsField.getDataType(),
            multiTypeEsField.getProperties()
        );
    }

    public static PotentiallyUnmappedEsField simpleResolution(Expression unmappedConv, Expression mappedConv, String name) {
        if (unmappedConv.dataType() != mappedConv.dataType()) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Both conversions must have the same target type, but got [%s, %s]",
                    unmappedConv.dataType(),
                    mappedConv.dataType()
                )
            );
        }
        if (unmappedConv.children().get(0).dataType() != KEYWORD) {
            throw new IllegalArgumentException(
                Strings.format("Unmapped conversion must be from keyword, but got [%s]", unmappedConv.children().get(0).dataType())
            );
        }
        if (mappedConv.children().get(0).dataType() == KEYWORD) {
            throw new IllegalArgumentException(Strings.format("Unmapped conversion must be from non-keyword"));
        }
        return new PotentiallyUnmappedEsField(new SimpleResolution(unmappedConv, mappedConv), name, unmappedConv.dataType(), Map.of());
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        switch (state) {
            case KeywordResolved unused -> {
                out.writeInt(0);
            }
            case SimpleConflict sf -> {
                out.writeInt(1);
                sf.otherType().writeTo(out);
            }
            case SimpleResolution sr -> {
                out.writeInt(2);
                out.writeNamedWriteable(sr.unmappedConversion());
                out.writeNamedWriteable(sr.mappedConversion());
            }
            case Invalid invalid -> {
                out.writeInt(3);
                invalid.invalidMappedField().writeTo(out);
            }
            case MultiType mt -> {
                out.writeInt(4);
                out.writeNamedWriteable(mt.conversionFromKeyword());
                mt.multiTypeEsField.writeTo(out);
            }
        }
        writeCachedStringWithVersionCheck(out, getName());
        getDataType().writeTo(out);
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
    }

    PotentiallyUnmappedEsField(StreamInput in) throws IOException {
        this(readState(in), readCachedStringWithVersionCheck(in), DataType.readFrom(in), in.readImmutableMap(EsField::readFrom));
    }

    private static State readState(StreamInput in) throws IOException {
        var ordinal = in.readInt();
        return switch (ordinal) {
            case 0 -> KeywordResolved.INSTANCE;
            case 1 -> new SimpleConflict(DataType.readFrom(in));
            case 2 -> new SimpleResolution(in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
            case 3 -> new Invalid(InvalidMappedField.readFrom(in));
            case 4 -> new MultiType(in.readNamedWriteable(Expression.class), MultiTypeEsField.readFrom(in));
            default -> throw new AssertionError("Unexpected ordinal: " + ordinal);
        };
    }

    @Override
    public String getWriteableName() {
        return "PotentiallyUnmappedEsField";
    }

    @Override
    public String toString() {
        return Strings.format("PotentiallyUnmappedEsField{state=%s}", state);
    }
}
