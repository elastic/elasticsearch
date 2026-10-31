/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOC_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLATTENED;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.PARTIAL_AGG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SOURCE;
import static org.elasticsearch.xpack.esql.core.type.DataType.TDIGEST;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.TSID_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.INNER;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

public class Join extends BinaryPlan
    implements
        PostAnalysisVerificationAware,
        SortAgnostic,
        ExecutesOn,
        PostOptimizationVerificationAware,
        TelemetryAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Join", Join::new);
    private static final TransportVersion ESQL_LOOKUP_JOIN_PRE_JOIN_FILTER = TransportVersion.fromName("esql_lookup_join_pre_join_filter");
    public static final DataType[] UNSUPPORTED_TYPES = {
        TEXT,
        VERSION,
        UNSIGNED_LONG,
        GEO_POINT,
        GEO_SHAPE,
        CARTESIAN_POINT,
        CARTESIAN_SHAPE,
        GEOHASH,
        GEOTILE,
        GEOHEX,
        UNSUPPORTED,
        NULL,
        COUNTER_LONG,
        COUNTER_INTEGER,
        COUNTER_DOUBLE,
        FLATTENED,
        OBJECT,
        SOURCE,
        DATE_PERIOD,
        TIME_DURATION,
        DOC_DATA_TYPE,
        TSID_DATA_TYPE,
        AGGREGATE_METRIC_DOUBLE,
        EXPONENTIAL_HISTOGRAM,
        TDIGEST,
        HISTOGRAM,
        DENSE_VECTOR,
        DATE_RANGE,
        DOUBLE_RANGE,
        PARTIAL_AGG };

    private final JoinConfig config;
    private List<Attribute> lazyOutput;
    // Where this join executes — relevant only on the coordinating node, thus transient.
    private final transient ExecuteLocation mode;

    public Join(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config, ExecuteLocation mode) {
        super(source, left, right);
        this.config = config;
        this.mode = mode;
    }

    public Join(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions,
        ExecuteLocation mode
    ) {
        this(source, left, right, new JoinConfig(type, leftFields, rightFields, joinOnConditions), mode);
    }

    public Join(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class), in.readNamedWriteable(LogicalPlan.class));
        this.config = new JoinConfig(in);
        this.mode = ExecuteLocation.ANY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(getRightToSerialize(out));
        config.writeTo(out);
    }

    protected LogicalPlan getRightToSerialize(StreamOutput out) {
        LogicalPlan rightToSerialize = right();
        if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_PRE_JOIN_FILTER) == false) {
            // Prior to TransportVersions.ESQL_LOOKUP_JOIN_PRE_JOIN_FILTER
            // we do not support a filter on top of the right side of the join
            // As we consider the filters optional, we remove them here
            while (rightToSerialize instanceof Filter filter) {
                rightToSerialize = filter.child();
            }
        }
        return rightToSerialize;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public JoinConfig config() {
        return config;
    }

    @Override
    protected NodeInfo<Join> info() {
        // Do not just add the JoinConfig as a whole - this would prevent correctly registering the
        // expressions and references.
        return NodeInfo.create(
            this,
            Join::new,
            left(),
            right(),
            config.type(),
            config.leftFields(),
            config.rightFields(),
            config.joinOnConditions(),
            mode
        );
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = Expressions.asAttributes(computeOutputExpressions(left().output(), right().output()));
        }
        return lazyOutput;
    }

    @Override
    public AttributeSet leftReferences() {
        return Expressions.references(config().leftFields());
    }

    @Override
    public AttributeSet rightReferences() {
        return Expressions.references(config().rightFields());
    }

    /**
     * The output fields obtained from the right child.
     */
    public List<Attribute> rightOutputFields() {
        AttributeSet leftInputs = left().outputSet();

        List<Attribute> rightOutputFields = new ArrayList<>();
        for (Attribute attr : output()) {
            if (leftInputs.contains(attr) == false) {
                rightOutputFields.add(attr);
            }
        }

        return rightOutputFields;
    }

    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return computeOutputExpressions(left, right, config);
    }

    /**
     * Combine the two lists of attributes into one.
     * In case of (name) conflicts, specify which sides wins, that is overrides the other column - the left or the right.
     */
    public static List<NamedExpression> computeOutputExpressions(
        List<? extends NamedExpression> leftOutput,
        List<? extends NamedExpression> rightOutput,
        JoinConfig config
    ) {
        JoinType joinType = config.type();
        List<NamedExpression> output;
        // TODO: make the other side nullable
        if (LEFT.equals(joinType)) {
            if (config.joinOnConditions() == null) {
                // right side becomes nullable and overrides left except for join keys, which we preserve from the left
                AttributeSet rightKeys = AttributeSet.of(config.rightFields());
                List<? extends NamedExpression> rightOutputWithoutMatchFields = rightOutput.stream()
                    .filter(ne -> rightKeys.contains(ne.toAttribute()) == false)
                    .toList();
                output = mergeOutputExpressions(rightOutputWithoutMatchFields, leftOutput);
            } else {
                // We don't allow any attributes in the joinOnConditions that don't have unique names
                // so right always overwrites left in case of name clashes
                output = mergeOutputExpressions(rightOutput, leftOutput);
            }

        } else if (INNER.equals(joinType) && config.joinOnConditions() == null) {
            // USING semantics, as for LEFT: the join keys are kept from the left, the right side overrides the rest
            AttributeSet rightKeys = AttributeSet.of(config.rightFields());
            List<? extends NamedExpression> rightOutputWithoutMatchFields = rightOutput.stream()
                .filter(ne -> rightKeys.contains(ne.toAttribute()) == false)
                .toList();
            output = mergeOutputExpressions(rightOutputWithoutMatchFields, leftOutput);
        } else {
            throw new IllegalArgumentException(joinType.joinName() + " unsupported");
        }
        return output;
    }

    /**
     * Make fields references, so we don't check if they exist in the index.
     * We do this for fields that we know don't come from the index.
     * <p>
     *   It's important that name is returned as a *reference* here
     *   instead of a field. If it were a field we'd use SearchStats
     *   on it and discover that it doesn't exist in the index. It doesn't!
     *   We don't expect it to. It exists only in the lookup table.
     *   TODO we should rework stats so we don't have to do this
     * </p>
     */
    public static List<Attribute> makeReference(List<Attribute> output) {
        List<Attribute> out = new ArrayList<>(output.size());
        for (Attribute a : output) {
            if (a.resolved() && a instanceof ReferenceAttribute == false) {
                out.add(new ReferenceAttribute(a.source(), a.qualifier(), a.name(), a.dataType(), a.nullable(), a.id(), a.synthetic()));
            } else {
                out.add(a);
            }
        }
        return out;
    }

    @Override
    public boolean expressionsResolved() {
        return config.expressionsResolved();
    }

    @Override
    public boolean resolved() {
        // resolve the join if
        // - the children are resolved
        // - the condition (if present) is resolved to a boolean
        return childrenResolved() && expressionsResolved();
    }

    /**
     * Whether this is the {@code JOIN <index> ON <field>} command: an INNER join between two indices. The parser builds it
     * as a plain {@link Join}; how it executes is chosen on the coordinator before mapping, which replaces it with a
     * {@link ColocatedJoin} when both indices are partitioned and placed alike (see {@code SelectJoinStrategy}).
     */
    public boolean isCandicateColocatedJoin() {
        return getClass() == Join.class && INNER.equals(config.type()) && config.joinOnConditions() == null;
    }

    /**
     * Only the {@code JOIN} command is counted here; other joins are planning artifacts of other commands.
     */
    @Override
    public String telemetryLabel() {
        return isCandicateColocatedJoin() ? "JOIN" : null;
    }

    public Join withConfig(JoinConfig config) {
        return new Join(source(), left(), right(), config, mode);
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new Join(source(), left, right, config, mode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, left(), right(), mode);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Join other = (Join) obj;
        return config.equals(other.config)
            && Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right())
            && mode == other.mode;
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (isCandicateColocatedJoin()) {
            verifyIndexJoin(failures);
        }
        for (int i = 0; i < config.leftFields().size(); i++) {
            Attribute leftField = config.leftFields().get(i);
            Attribute rightField = config.rightFields().get(i);
            if (comparableTypes(leftField, rightField) == false) {
                failures.add(
                    fail(
                        leftField,
                        "JOIN left field [{}] of type [{}] is incompatible with right field [{}] of type [{}]",
                        leftField.name(),
                        leftField.dataType(),
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
            // TODO: Add support for VERSION by implementing QueryList.versionTermQueryList similar to ipTermQueryList
            if (Arrays.stream(UNSUPPORTED_TYPES).anyMatch(t -> rightField.dataType().equals(t))) {
                failures.add(
                    fail(leftField, "JOIN with right field [{}] of type [{}] is not supported", rightField.name(), rightField.dataType())
                );
            }
        }
    }

    /**
     * Types a {@code JOIN} key can have: those of the fields that {@code index.routing_field} can name (short and byte fields
     * are integers here). Both indices route the canonical form of the value, so equal keys share a shard number; together
     * with the type compatibility check this keeps keyword, integral and ip keys apart.
     */
    private static final Set<DataType> ROUTING_KEY_TYPES = Set.of(DataType.KEYWORD, DataType.LONG, DataType.INTEGER, DataType.IP);

    /**
     * Checks of the {@code JOIN} command that do not depend on where shards are: the right side is one local index and the
     * key has a type that can be a routing field. Shard counts, routing and placement are checked when the execution
     * strategy is chosen.
     */
    private void verifyIndexJoin(Failures failures) {
        if (right() instanceof EsRelation relation) {
            List<String> local = relation.concreteIndices().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            if (relation.concreteIndices().size() != 1 || local == null || local.size() != 1) {
                failures.add(
                    fail(
                        this,
                        "JOIN requires [{}] to resolve to a single local index, found {}",
                        relation.indexPattern(),
                        relation.concreteIndices()
                    )
                );
            }
        } else {
            failures.add(fail(this, "JOIN requires an index on the right side"));
        }
        for (int i = 0; i < config.leftFields().size(); i++) {
            Attribute leftField = config.leftFields().get(i);
            Attribute rightField = config.rightFields().get(i);
            if (ROUTING_KEY_TYPES.contains(leftField.dataType()) == false || ROUTING_KEY_TYPES.contains(rightField.dataType()) == false) {
                failures.add(
                    fail(
                        leftField,
                        "JOIN key must be a keyword, long, integer or ip field on both sides, found [{}] and [{}]",
                        leftField.dataType(),
                        rightField.dataType()
                    )
                );
            }
        }
    }

    private static boolean comparableTypes(Attribute left, Attribute right) {
        DataType leftType = left.dataType();
        DataType rightType = right.dataType();
        if (leftType == NULL) {
            // A field can have NULL type when UNMAPPED_FIELDS="NULLIFY" resolves a missing field to null.
            // Only the left side needs checking: lookup indices are excluded from nullification (see ResolveUnmapped#nullify),
            // and for INLINE STATS the right side's grouping key inherits its type from the left.
            return true;
        }
        if (leftType.isNumeric() && rightType.isNumeric()) {
            // Allow byte, short, integer, long, half_float, scaled_float, float and double to join against each other
            return commonType(leftType, rightType) != null;
        }
        return leftType.noText() == rightType.noText();
    }

    @Override
    public ExecuteLocation executesOn() {
        return mode;
    }

    private void checkRemoteJoin(Failures failures) {
        checkForRemoteJoinBlockers(this, failures);
    }

    private void checkForRemoteJoinBlockers(LogicalPlan plan, Failures failures) {
        if (plan instanceof AbstractSubqueryJoin subqueryJoin) {
            // The right side is an independent subquery; do not traverse into it.
            checkForRemoteJoinBlockers(subqueryJoin.left(), failures);
        } else {
            if (plan != this) {
                if (plan instanceof Limit == false) {
                    // Limit is ok because it can be moved in by the optimizer
                    // We check LIMITs in LookupJoin pre-optimization so they are still not allowed there
                    if (plan instanceof PipelineBreaker
                        || (plan instanceof ExecutesOn ex && ex.executesOn() == ExecuteLocation.COORDINATOR)) {
                        failures.add(
                            fail(
                                this,
                                "LOOKUP JOIN with remote indices can't be executed after ["
                                    + plan.source().text()
                                    + "]"
                                    + plan.source().source()
                            )
                        );
                    }
                }
            }
            for (LogicalPlan child : plan.children()) {
                checkForRemoteJoinBlockers(child, failures);
            }
        }
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        if (executesOn() == ExecuteLocation.REMOTE) {
            checkRemoteJoin(failures);
        }
    }
}
