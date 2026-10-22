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
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class HashJoinExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "HashJoinExec",
        HashJoinExec::new
    );
    private static final TransportVersion ESQL_JOIN_SUBQUERY = Analyzer.ESQL_JOIN_SUBQUERY;
    private static final TransportVersion ESQL_LOOKUP_JOIN_ON_EXPRESSION = TransportVersion.fromName("esql_lookup_join_on_expression");

    /**
     * The join this hash join implements. All modes share the probe operator but differ in build-key cardinality, in what happens to
     * unmatched left rows and in the column order they promise to their logical join.
     */
    public enum Mode {
        /**
         * Each build key matches at most one row and unmatched left rows get null build columns. The join keys move after the build
         * columns, matching the INLINE STATS output.
         */
        INLINE_STATS,
        /**
         * Build keys may repeat, emitting one row per match, and unmatched left rows get null build columns. Columns follow LOOKUP JOIN
         * order: the left columns, minus those shadowed by name, then the added build columns.
         */
        LEFT_OUTER,
        /**
         * Like {@link #LEFT_OUTER}, but unmatched left rows are dropped right after the lookup, before any build column is loaded.
         */
        INNER
    }

    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final List<Attribute> addedFields;
    private final Mode mode;
    private List<Attribute> lazyOutput;
    private AttributeSet lazyAddedFields;

    public HashJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan hashData,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields
    ) {
        this(source, left, hashData, leftFields, rightFields, addedFields, Mode.INLINE_STATS);
    }

    public HashJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan hashData,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        Mode mode
    ) {
        super(source, left, hashData);
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.addedFields = addedFields;
        this.mode = mode;
    }

    private HashJoinExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION) == false) {
            in.readNamedWriteableCollectionAsList(Attribute.class);
        }
        this.leftFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.rightFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.addedFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.mode = in.getTransportVersion().supports(ESQL_JOIN_SUBQUERY) ? in.readEnum(Mode.class) : Mode.INLINE_STATS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (mode != Mode.INLINE_STATS && out.getTransportVersion().supports(ESQL_JOIN_SUBQUERY) == false) {
            throw new IllegalStateException("subquery hash join cannot be sent to a node without subquery join support");
        }
        super.writeTo(out);
        if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION) == false) {
            out.writeNamedWriteableCollection(leftFields);
        }
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        out.writeNamedWriteableCollection(addedFields);
        if (out.getTransportVersion().supports(ESQL_JOIN_SUBQUERY)) {
            out.writeEnum(mode);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public PhysicalPlan joinData() {
        return right();
    }

    public List<Attribute> leftFields() {
        return leftFields;
    }

    public List<Attribute> rightFields() {
        return rightFields;
    }

    public Mode mode() {
        return mode;
    }

    public Set<Attribute> addedFields() {
        if (lazyAddedFields == null) {
            lazyAddedFields = AttributeSet.of(addedFields);
        }
        return lazyAddedFields;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, addedFields);
        return this;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = switch (mode) {
                case INLINE_STATS -> keysLastOutput();
                case LEFT_OUTER, INNER -> leftFirstOutput();
            };
        }
        return lazyOutput;
    }

    private List<Attribute> keysLastOutput() {
        List<Attribute> leftOutputWithoutKeys = left().output().stream().filter(attr -> leftFields.contains(attr) == false).toList();
        List<Attribute> rightWithAppendedKeys = new ArrayList<>(right().output());
        rightWithAppendedKeys.removeAll(rightFields);
        rightWithAppendedKeys.addAll(leftFields);

        for (Attribute f : addedFields) {
            if (right().outputSet().contains(f) == false) {
                rightWithAppendedKeys.add(f);
            }
        }

        return mergeOutputAttributes(rightWithAppendedKeys, leftOutputWithoutKeys);
    }

    private List<Attribute> leftFirstOutput() {
        Set<String> addedFieldNames = new HashSet<>(Expressions.names(addedFields));
        List<Attribute> output = new ArrayList<>(left().output());
        output.removeIf(a -> addedFieldNames.contains(a.name()));
        output.addAll(addedFields);
        return output;
    }

    @Override
    public AttributeSet inputSet() {
        // TODO: this is a hack until qualifiers land since the right side is always materialized
        return left().outputSet();
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet leftReferences() {
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet rightReferences() {
        return Expressions.references(rightFields);
    }

    @Override
    public HashJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new HashJoinExec(source(), left, right, leftFields, rightFields, addedFields, mode);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, HashJoinExec::new, left(), right(), leftFields, rightFields, addedFields, mode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        HashJoinExec hash = (HashJoinExec) o;
        return leftFields.equals(hash.leftFields)
            && rightFields.equals(hash.rightFields)
            && addedFields.equals(hash.addedFields)
            && mode == hash.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), leftFields, rightFields, addedFields, mode);
    }
}
