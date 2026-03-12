/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.SideChannel;
import org.elasticsearch.compute.operator.topn.DocVectorEncoder;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.Limit.ESQL_LIMIT_BY;

public class TopNExec extends UnaryExec implements EstimatesRowSize {
    private static final TransportVersion ESQL_TOPN_AVOID_RESORTING = TransportVersion.fromName("esql_topn_avoid_resorting");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TopNExec",
        TopNExec::new
    );

    private final Expression limit;
    private final List<Order> order;
    /**
     * Attributes that may be extracted as doc values even if that makes them
     * less accurate. This is mostly used for geo fields which lose a lot of
     * precision in their doc values, but in some cases doc values provides
     * <strong>enough</strong> precision to do the job.
     * <p>
     * This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> docValuesAttributes;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    private List<Expression> groupings;

    private final InputOrdering inputOrdering;

    /**
     * Optional {@link SideChannel} for passing information about the minimum competitive
     * match back to the source operator.
     */
    @Nullable
    private final transient SharedMinCompetitive.Supplier minCompetitive;

    public TopNExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limit,
        List<Expression> groupings,
        Integer estimatedRowSize
    ) {
        this(source, child, order, limit, estimatedRowSize, groupings, Set.of(), InputOrdering.NOT_SORTED, null);
    }

    private TopNExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limit,
        Integer estimatedRowSize,
        List<Expression> groupings,
        InputOrdering inputOrdering
    ) {
        this(source, child, order, limit, estimatedRowSize, groupings, Set.of(), inputOrdering, null);
    }

    private TopNExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limit,
        Integer estimatedRowSize,
        List<Expression> groupings,
        Set<Attribute> docValuesAttributes,
        InputOrdering inputOrdering,
        @Nullable SharedMinCompetitive.Supplier minCompetitive
    ) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.estimatedRowSize = estimatedRowSize;
        this.inputOrdering = inputOrdering;
        this.docValuesAttributes = docValuesAttributes;
        this.groupings = groupings;
        this.minCompetitive = minCompetitive;
    }

    private TopNExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readCollectionAsList(org.elasticsearch.xpack.esql.expression.Order::new),
            in.readNamedWriteable(Expression.class),
            in.readOptionalVInt(),
            List.of(),
            in.getTransportVersion().supports(ESQL_TOPN_AVOID_RESORTING) ? InputOrdering.valueOf(in.readString()) : InputOrdering.NOT_SORTED
        );

        if (in.getTransportVersion().supports(ESQL_LIMIT_BY)) {
            this.groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        }
        // docValueAttributes are only used on the data node and never serialized.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order());
        out.writeNamedWriteable(limit());
        out.writeOptionalVInt(estimatedRowSize());

        if (out.getTransportVersion().supports(ESQL_TOPN_AVOID_RESORTING)) {
            out.writeString(inputOrdering.toString());
        }

        if (out.getTransportVersion().supports(ESQL_LIMIT_BY)) {
            out.writeNamedWriteableCollection(groupings());
        } else if (groupings.isEmpty() == false) {
            throw new IllegalArgumentException("LIMIT BY is not supported by all nodes in the cluster");
        }

        // docValueAttributes are only used on the data node and never serialized.
        if (minCompetitive != null) {
            throw new IllegalStateException("min competitive should not be set on the coordinating node because it is not serialized");
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<TopNExec> info() {
        return NodeInfo.create(this, TopNExec::new, child(), order, limit, groupings, estimatedRowSize);
    }

    @Override
    public TopNExec replaceChild(PhysicalPlan newChild) {
        return new TopNExec(
            source(),
            newChild,
            order,
            limit,
            estimatedRowSize,
            groupings,
            docValuesAttributes,
            inputOrdering,
            minCompetitive
        );
    }

    public TopNExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new TopNExec(
            source(),
            child(),
            order,
            limit,
            estimatedRowSize,
            groupings,
            docValuesAttributes,
            inputOrdering,
            minCompetitive
        );
    }

    public TopNExec withSortedInput() {
        return new TopNExec(
            source(),
            child(),
            order,
            limit,
            estimatedRowSize,
            groupings,
            docValuesAttributes,
            InputOrdering.SORTED,
            minCompetitive
        );
    }

    public TopNExec withNonSortedInput() {
        return new TopNExec(
            source(),
            child(),
            order,
            limit,
            estimatedRowSize,
            groupings,
            docValuesAttributes,
            InputOrdering.NOT_SORTED,
            minCompetitive
        );
    }

    public SharedMinCompetitive.Supplier minCompetitive() {
        return minCompetitive;
    }

    public List<SharedMinCompetitive.KeyConfig> minCompetitiveKeyConfig() {
        return order.stream().map(o -> {
            TopNEncoder encoder = keyEncoder(o.child().dataType());
            if (encoder == null) {
                throw new IllegalStateException("[" + o.child().dataType() + "] is not a valid key");
            }
            return new SharedMinCompetitive.KeyConfig(
                keyElementType(o.child().dataType()),
                encoder,
                o.direction() == Order.OrderDirection.ASC,
                o.nullsPosition() == Order.NullsPosition.FIRST
            );
        }).toList();
    }

    public TopNExec withMinCompetitive(SharedMinCompetitive.Supplier minCompetitive) {
        return new TopNExec(
            source(),
            child(),
            order,
            limit,
            estimatedRowSize,
            groupings,
            docValuesAttributes,
            inputOrdering,
            minCompetitive
        );
    }

    public Expression limit() {
        return limit;
    }

    public List<Order> order() {
        return order;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Set<Attribute> docValuesAttributes() {
        return docValuesAttributes;
    }

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        final List<Attribute> output = output();
        final boolean needsSortedDocIds = output.stream().anyMatch(a -> a.dataType() == DataType.DOC_DATA_TYPE);
        state.add(needsSortedDocIds, output);
        int size = state.consumeAllFields(true);
        size = Math.max(size, 1);
        return Objects.equals(this.estimatedRowSize, size)
            ? this
            : new TopNExec(source(), child(), order, limit, size, groupings, docValuesAttributes, inputOrdering, minCompetitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            order,
            limit,
            estimatedRowSize,
            groupings,
            docValuesAttributes,
            inputOrdering,
            minCompetitive
        );
    }

    @Override
    public boolean equals(Object obj) {
        boolean equals = super.equals(obj);
        if (equals) {
            var other = (TopNExec) obj;
            equals = Objects.equals(order, other.order)
                && Objects.equals(limit, other.limit)
                && Objects.equals(estimatedRowSize, other.estimatedRowSize)
                && Objects.equals(docValuesAttributes, other.docValuesAttributes)
                && Objects.equals(groupings, other.groupings)
                && Objects.equals(inputOrdering, other.inputOrdering)
                /*
                 * NOTE: minCompetitive only has reference equality. We don't serialize
                 * it, but never set it on the coordinating node. It is always null there.
                 * But it *is* important that it be in equals and hashCode - the rewrites
                 * don't replace the result if it isn't.
                 */
                && Objects.equals(minCompetitive, other.minCompetitive);
        }
        return equals;
    }

    public InputOrdering inputOrdering() {
        return inputOrdering;
    }

    private static ElementType keyElementType(DataType type) {
        return PlannerUtils.toElementType(type, MappedFieldType.FieldExtractPreference.NONE);
    }

    /**
     * The encoder to use when a field is included in the sort.
     * <p>
     *     This is essentially one big {@code switch} statement on {@code type}. It intentionally
     *     doesn't have a {@code default} and shouldn't add one. We want to make sure that folks
     *     who add a new type think about sorting.
     * </p>
     * <p>
     *     While a type is {@code underConstruction} its <strong>fine</strong> if the {@code switch}
     *     throws an {@link IllegalStateException}. But before the type is released we need to support
     *     the type.
     * </p>
     */
    public static TopNEncoder encoder(DataType type, IndexedByShardId<? extends RefCounted> shardContexts) {
        TopNEncoder encoder = switch (type) {
            // HEY! If you see a compilation failure on this switch read the method javadoc.
            case IP -> TopNEncoder.IP;
            case TEXT, KEYWORD -> TopNEncoder.UTF8;
            case VERSION -> TopNEncoder.VERSION;
            case DOC_DATA_TYPE -> new DocVectorEncoder(shardContexts);
            case BOOLEAN, NULL, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, HALF_FLOAT, DATETIME, DATE_NANOS, DATE_PERIOD, TIME_DURATION,
                OBJECT, SCALED_FLOAT, UNSIGNED_LONG -> TopNEncoder.DEFAULT_SORTABLE;
            case GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, SOURCE,
                AGGREGATE_METRIC_DOUBLE, DENSE_VECTOR, GEOHASH, GEOTILE, GEOHEX, EXPONENTIAL_HISTOGRAM, TDIGEST, HISTOGRAM, TSID_DATA_TYPE,
                DATE_RANGE -> TopNEncoder.DEFAULT_UNSORTABLE;
            case UNSUPPORTED -> TopNEncoder.UNSUPPORTED;
        };
        if (Assertions.ENABLED) {
            TopNEncoder keyEncoder = keyEncoder(type);
            if (keyEncoder != null) {
                if (keyEncoder != encoder) {
                    throw new IllegalStateException("encoder must align with keyEncoder");
                }
            }
        }
        return encoder;
    }

    /**
     * @return the encoder used for decoding {@link SharedMinCompetitive} or {@code null} if the
     *         type doesn't support being a key.
     */
    @Nullable
    private static TopNEncoder keyEncoder(DataType type) {
        return switch (type) {
            case IP -> TopNEncoder.IP;
            case TEXT, KEYWORD -> TopNEncoder.UTF8;
            case VERSION -> TopNEncoder.VERSION;
            case BOOLEAN, NULL, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, HALF_FLOAT, DATETIME, DATE_NANOS, DATE_PERIOD, TIME_DURATION,
                OBJECT, SCALED_FLOAT, UNSIGNED_LONG -> TopNEncoder.DEFAULT_SORTABLE;
            default -> null;
        };
    }
}
