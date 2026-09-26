/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Named-dimension primitives intended to back native PackDims/UnpackDims execution and runtime dimension loading
 * with the same packed representation. Migration of the existing positional execution paths is still pending;
 * these primitives are not intended to form a separate packing subsystem.
 * Dimension descriptors carry names and extraction types but are not dependencies on page columns. Only children
 * are evaluated. SET preserves null and empty strings; structural removal is exclusively UNSET.
 */
public final class PackDimSupport extends EsqlScalarFunction implements VersionedNamedWriteable {
    public static final TransportVersion ESQL_PACK_DIM = TransportVersion.fromName("esql_pack_dim_buf");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PackDim",
        PackDimSupport::new
    );

    /** Operations with one output column; unpacking is expressed as a projection of GET expressions. */
    public enum Operation {
        PACK,
        GET,
        SET,
        UNSET,
        SET_FROM_DIMENSIONS,
        MAP_FROM_DIMENSIONS
    }

    private final Operation operation;
    private final List<Attribute> dimensions;

    private PackDimSupport(Source source, Operation operation, List<Attribute> dimensions, List<Expression> children) {
        super(source, children);
        this.operation = Objects.requireNonNull(operation);
        this.dimensions = List.copyOf(dimensions);
        int expected = switch (operation) {
            case PACK -> dimensions.size();
            case GET, UNSET -> 1;
            case SET, SET_FROM_DIMENSIONS, MAP_FROM_DIMENSIONS -> 2;
        };
        if (children.size() != expected
            || ((operation == Operation.GET || operation == Operation.SET || operation == Operation.SET_FROM_DIMENSIONS)
                && dimensions.size() != 1)) {
            throw new IllegalArgumentException("invalid PackDim operation " + operation);
        }
        var names = new HashSet<String>();
        for (Attribute dimension : dimensions) {
            if (names.add(dimension.name()) == false) throw new IllegalArgumentException("duplicate dimension [" + dimension.name() + "]");
        }
    }

    private PackDimSupport(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readEnum(Operation.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    /** Constructs records from named expressions, preserving each provided key even when its value is null. */
    public static PackDimSupport pack(Source source, List<? extends NamedExpression> dimensions) {
        return new PackDimSupport(
            source,
            Operation.PACK,
            dimensions.stream().map(NamedExpression::toAttribute).toList(),
            dimensions.stream().map(PackDimSupport::valueExpression).toList()
        );
    }

    /** Extracts a literal dimension name using its declared output type, without referencing an external column. */
    public static PackDimSupport get(Expression packed, Attribute dimension) {
        return new PackDimSupport(packed.source(), Operation.GET, List.of(dimension), List.of(packed));
    }

    /** Inserts or replaces a named dimension. Null is a present value, not a deletion sentinel. */
    public static PackDimSupport set(Expression packed, NamedExpression dimension) {
        return new PackDimSupport(
            packed.source(),
            Operation.SET,
            List.of(dimension.toAttribute()),
            List.of(packed, valueExpression(dimension))
        );
    }

    /**
     * Sets a dimension using a deterministic expression of this packed input and literals only.
     * The planner must establish determinism; compilation rejects external column dependencies. Evaluation
     * preserves the ordinary scalar tree, including lazy branches, but runs once per live record.
     */
    public static PackDimSupport setFromDimensions(Expression packed, NamedExpression dimension) {
        return new PackDimSupport(
            packed.source(),
            Operation.SET_FROM_DIMENSIONS,
            List.of(dimension.toAttribute()),
            List.of(packed, valueExpression(dimension))
        );
    }

    /**
     * Evaluates a planner-proven deterministic packed expression per live record. Ordinary conditional expressions
     * select keep, Set or Unset without adding language-specific mutation semantics to the block.
     */
    public static PackDimSupport mapFromDimensions(Expression packed, Expression result) {
        return new PackDimSupport(packed.source(), Operation.MAP_FROM_DIMENSIONS, List.of(), List.of(packed, result));
    }

    /** Removes named dimensions in one traversal; absent keys are no-ops and an empty record stays non-null. */
    public static PackDimSupport unset(Expression packed, List<? extends Attribute> dimensions) {
        return new PackDimSupport(packed.source(), Operation.UNSET, List.copyOf(dimensions), List.of(packed));
    }

    /**
     * Produces named scalar projections for a planner Eval/Project pair. This is not a multicolumn scalar and does
     * not modify the legacy UnpackDims operator. Output attributes retain the requested dimension IDs.
     */
    public static List<Alias> unpack(Expression packed, List<? extends Attribute> dimensions) {
        return dimensions.stream().map(d -> new Alias(packed.source(), d.name(), get(packed, d), d.id())).toList();
    }

    private static Expression valueExpression(NamedExpression dimension) {
        Expression value = dimension;
        while (value instanceof Alias alias)
            value = alias.child();
        return value;
    }

    public Operation operation() {
        return operation;
    }

    public List<Attribute> dimensions() {
        return dimensions;
    }

    @Override
    public String functionName() {
        return switch (operation) {
            case PACK -> "PackDim_Pack";
            case GET -> "PackDim_Get";
            case SET, SET_FROM_DIMENSIONS -> "PackDim_Set";
            case UNSET -> "PACK_DIM_UNSET";
            case MAP_FROM_DIMENSIONS -> "PackDim_Map";
        };
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // Dimension descriptors are deliberately not children/dependencies, but must remain visible in plans.
        sb.append(functionName()).append('(');
        if (operation != Operation.PACK) children().getFirst().nodeString(sb, format, mapper);
        if (operation == Operation.MAP_FROM_DIMENSIONS) {
            sb.append(',');
            children().get(1).nodeString(sb, format, mapper);
        }
        for (int i = 0; i < dimensions.size(); i++) {
            if (operation != Operation.PACK || i > 0) sb.append(',');
            dimensions.get(i).nodeString(sb, format, mapper);
            if (operation == Operation.PACK || operation == Operation.SET || operation == Operation.SET_FROM_DIMENSIONS) {
                sb.append(":=");
                children().get(operation == Operation.PACK ? i : 1).nodeString(sb, format, mapper);
            }
        }
        sb.append(')');
    }

    @Override
    public DataType dataType() {
        return operation == Operation.GET ? dimensions.getFirst().dataType() : DataType.PACK_DIM;
    }

    @Override
    public Nullability nullable() {
        return operation == Operation.PACK ? Nullability.FALSE : Nullability.TRUE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) return new TypeResolution("Unresolved children");
        if (operation != Operation.PACK && children().getFirst().dataType() != DataType.PACK_DIM) {
            return new TypeResolution(functionName() + " requires packed dimensions");
        }
        if (operation == Operation.MAP_FROM_DIMENSIONS && children().get(1).dataType() != DataType.PACK_DIM) {
            return new TypeResolution("record mapping must return packed dimensions");
        }
        if (operation != Operation.UNSET) {
            for (int i = 0; i < dimensions.size(); i++) {
                DataType type = dimensions.get(i).dataType();
                if (supported(type) == false) return new TypeResolution("unsupported dimension type [" + type + "]");
                if (operation == Operation.PACK && children().get(i).dataType() != type
                    || (operation == Operation.SET || operation == Operation.SET_FROM_DIMENSIONS) && children().get(1).dataType() != type) {
                    return new TypeResolution("dimension expression and descriptor types differ");
                }
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    private static boolean supported(DataType type) {
        return type == DataType.KEYWORD
            || type == DataType.BOOLEAN
            || type == DataType.INTEGER
            || type == DataType.LONG
            || type == DataType.DOUBLE
            || type == DataType.NULL;
    }

    @Override
    public Expression replaceChildren(List<Expression> children) {
        return new PackDimSupport(source(), operation, dimensions, children);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, PackDimSupport::new, operation, dimensions, children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_PACK_DIM;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_PACK_DIM) == false) throw new IOException("PackDim requires " + ESQL_PACK_DIM);
        source().writeTo(out);
        out.writeEnum(operation);
        out.writeNamedWriteableCollection(dimensions);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return PackDimEvaluator.factory(this, toEvaluator);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && operation == ((PackDimSupport) other).operation
            && dimensions.equals(((PackDimSupport) other).dimensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation, dimensions);
    }
}
