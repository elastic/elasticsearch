/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.enrich.AbstractLookupService.LOOKUP_POSITIONS_FIELD;

/**
 * Physical plan node representing the merge/drop step of a lookup operation.
 * This handles either merging multiple result pages into one (via MergePositionsOperator)
 * or dropping the doc block (via ProjectOperator).
 * This is used internally by lookup services to represent the lookup structure
 * before converting to operators.
 */
public class LookupMergeDropExec extends UnaryExec {
    private final List<NamedExpression> extractFields;
    private final ElementType[] mergingTypes;
    private final int[] mergingChannels;
    private final boolean includePositions;

    public LookupMergeDropExec(
        Source source,
        PhysicalPlan child,
        List<NamedExpression> extractFields,
        ElementType[] mergingTypes,
        int[] mergingChannels,
        boolean includePositions
    ) {
        super(source, child);
        this.extractFields = extractFields;
        this.mergingTypes = mergingTypes;
        this.mergingChannels = mergingChannels;
        this.includePositions = includePositions;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<LookupMergeDropExec> info() {
        return NodeInfo.create(this, LookupMergeDropExec::new, child(), extractFields, mergingTypes, mergingChannels, includePositions);
    }

    @Override
    public LookupMergeDropExec replaceChild(PhysicalPlan newChild) {
        return new LookupMergeDropExec(source(), newChild, extractFields, mergingTypes, mergingChannels, includePositions);
    }

    public List<NamedExpression> extractFields() {
        return extractFields;
    }

    public ElementType[] mergingTypes() {
        return mergingTypes;
    }

    public int[] mergingChannels() {
        return mergingChannels;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public List<Attribute> output() {
        if (includePositions) {
            // For lookup joins: output includes positions + extractFields
            // Positions is an IntBlock created by EnrichQuerySourceOperator but not exposed as an attribute
            // Create a synthetic positions attribute
            List<Attribute> result = new ArrayList<>(1 + extractFields.size());
            FieldAttribute positionsAttribute = new FieldAttribute(Source.EMPTY, LOOKUP_POSITIONS_FIELD.getName(), LOOKUP_POSITIONS_FIELD);
            result.add(positionsAttribute);
            // Add extractFields
            for (NamedExpression extractField : extractFields) {
                result.add(extractField.toAttribute());
            }
            return result;
        } else {
            // For enrich lookups: output is only extractFields (MergePositionsOperator drops positions)
            return extractFields.stream().map(NamedExpression::toAttribute).toList();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        LookupMergeDropExec lookupMergeDropExec = (LookupMergeDropExec) o;
        return Objects.equals(extractFields, lookupMergeDropExec.extractFields)
            && java.util.Arrays.equals(mergingTypes, lookupMergeDropExec.mergingTypes)
            && java.util.Arrays.equals(mergingChannels, lookupMergeDropExec.mergingChannels)
            && includePositions == lookupMergeDropExec.includePositions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            extractFields,
            java.util.Arrays.hashCode(mergingTypes),
            java.util.Arrays.hashCode(mergingChannels),
            includePositions
        );
    }
}
