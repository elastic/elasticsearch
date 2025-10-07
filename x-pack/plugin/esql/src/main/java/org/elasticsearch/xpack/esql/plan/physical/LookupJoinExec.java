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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LookupJoinExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LookupJoinExec",
        LookupJoinExec::new
    );
    private static final TransportVersion ESQL_LOOKUP_JOIN_ON_EXPRESSION = TransportVersion.fromName("esql_lookup_join_on_expression");

    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    /**
     * These cannot be computed from the left + right outputs, because
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes} will replace the {@link EsSourceExec} on
     * the right hand side by a {@link EsQueryExec}, and thus lose the information of which fields we'll get from the lookup index.
     */
    private final List<Attribute> addedFields;
    private final Expression joinOnConditions;
    private List<Attribute> lazyOutput;

    public LookupJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan lookup,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        Expression joinOnConditions
    ) {
        super(source, left, lookup);
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.addedFields = addedFields;
        this.joinOnConditions = joinOnConditions;
    }

    private LookupJoinExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        this.leftFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.rightFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.addedFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
            this.joinOnConditions = in.readOptionalNamedWriteable(Expression.class);
        } else {
            this.joinOnConditions = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        out.writeNamedWriteableCollection(addedFields);
        if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
            out.writeOptionalNamedWriteable(joinOnConditions);
        } else if (joinOnConditions != null) {
            throw new IllegalArgumentException("LOOKUP JOIN with ON conditions is not supported on remote node");
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public PhysicalPlan lookup() {
        return right();
    }

    public List<Attribute> leftFields() {
        return leftFields;
    }

    public List<Attribute> rightFields() {
        return rightFields;
    }

    public List<Attribute> addedFields() {
        return addedFields;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = new ArrayList<>(left().output());
            var addedFieldsNames = addedFields.stream().map(Attribute::name).toList();
            lazyOutput.removeIf(a -> addedFieldsNames.contains(a.name()));
            lazyOutput.addAll(addedFields);
        }
        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, addedFields);
        return this;
    }

    @Override
    public AttributeSet inputSet() {
        // TODO: this is a hack since the right side is always materialized - instead this should
        // return the _doc so the extraction can happen lazily
        return left().outputSet();
    }

    @Override
    protected AttributeSet computeReferences() {
        // TODO: same as above - once lazy materialization of both sides lands, this needs updating
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet leftReferences() {
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet rightReferences() {
        // TODO: currently it's hard coded that we add all fields from the lookup index. But the output we "officially" get from the right
        // hand side is inconsistent:
        // - After logical optimization, there's a FragmentExec with an EsRelation on the right hand side with all the fields.
        // - After local physical optimization, there's just an EsQueryExec here, with no fields other than _doc mentioned and we don't
        // insert field extractions in the plan, either.
        return AttributeSet.EMPTY;
    }

    @Override
    public LookupJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new LookupJoinExec(source(), left, right, leftFields, rightFields, addedFields, joinOnConditions);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, LookupJoinExec::new, left(), right(), leftFields, rightFields, addedFields, joinOnConditions);
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
        LookupJoinExec other = (LookupJoinExec) o;
        return leftFields.equals(other.leftFields)
            && rightFields.equals(other.rightFields)
            && addedFields.equals(other.addedFields)
            && Objects.equals(joinOnConditions, other.joinOnConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), leftFields, rightFields, addedFields, joinOnConditions);
    }

    public boolean isOnJoinExpression() {
        return joinOnConditions != null;
    }

    public Expression joinOnConditions() {
        return joinOnConditions;
    }
}
