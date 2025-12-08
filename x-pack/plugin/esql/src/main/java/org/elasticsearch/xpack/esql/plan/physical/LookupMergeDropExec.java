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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    public LookupMergeDropExec(
        Source source,
        PhysicalPlan child,
        List<NamedExpression> extractFields,
        ElementType[] mergingTypes,
        int[] mergingChannels
    ) {
        super(source, child);
        this.extractFields = extractFields;
        this.mergingTypes = mergingTypes;
        this.mergingChannels = mergingChannels;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<LookupMergeDropExec> info() {
        return NodeInfo.create(this, LookupMergeDropExec::new, child(), extractFields, mergingTypes, mergingChannels);
    }

    @Override
    public LookupMergeDropExec replaceChild(PhysicalPlan newChild) {
        return new LookupMergeDropExec(source(), newChild, extractFields, mergingTypes, mergingChannels);
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
        List<Attribute> childOutput = child().output();
        if (extractFields.isEmpty()) {
            return childOutput;
        }
        List<Attribute> output = new ArrayList<>(childOutput.size() + extractFields.size());
        output.addAll(childOutput);
        output.addAll(extractFields.stream().map(NamedExpression::toAttribute).toList());
        return output;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        LookupMergeDropExec lookupMergeDropExec = (LookupMergeDropExec) o;
        return Objects.equals(extractFields, lookupMergeDropExec.extractFields)
            && java.util.Arrays.equals(mergingTypes, lookupMergeDropExec.mergingTypes)
            && java.util.Arrays.equals(mergingChannels, lookupMergeDropExec.mergingChannels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            extractFields,
            java.util.Arrays.hashCode(mergingTypes),
            java.util.Arrays.hashCode(mergingChannels)
        );
    }
}
