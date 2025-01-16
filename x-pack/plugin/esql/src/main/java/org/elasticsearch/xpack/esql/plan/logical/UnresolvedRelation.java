/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.TableIdentifier;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class UnresolvedRelation extends LeafPlan implements Unresolvable {

    private final TableIdentifier table;
    private final boolean frozen;
    private final List<Attribute> metadataFields;
    /*
     * Expected indexMode based on the declaration - used later for verification
     * at resolution time.
     */
    private final IndexMode indexMode;
    private final String unresolvedMsg;

    /**
     * Used by telemetry to say if this is the result of a FROM command
     * or a METRICS command (or maybe something else in the future)
     */
    private final String commandName;

    public UnresolvedRelation(
        Source source,
        TableIdentifier table,
        boolean frozen,
        List<Attribute> metadataFields,
        IndexMode indexMode,
        String unresolvedMessage,
        String commandName
    ) {
        super(source);
        this.table = table;
        this.frozen = frozen;
        this.metadataFields = metadataFields;
        this.indexMode = indexMode;
        this.unresolvedMsg = unresolvedMessage == null ? "Unknown index [" + table.index() + "]" : unresolvedMessage;
        this.commandName = commandName;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, UnresolvedRelation::new, table, frozen, metadataFields, indexMode, unresolvedMsg, commandName);
    }

    public TableIdentifier table() {
        return table;
    }

    public boolean frozen() {
        return frozen;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    /**
     *
     * This is used by {@link org.elasticsearch.xpack.esql.stats.PlanningMetrics} to collect query statistics
     * It can return
     * <ul>
     *     <li>"FROM" if this a <code>|FROM idx</code> command</li>
     *     <li>"FROM TS" if it is the result of a <code>| METRICS idx some_aggs() BY fields</code> command</li>
     *     <li>"METRICS" if it is the result of a <code>| METRICS idx</code> (no aggs, no groupings)</li>
     * </ul>
     */
    @Override
    public String commandName() {
        return commandName;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    public List<Attribute> metadataFields() {
        return metadataFields;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), table, metadataFields, indexMode, unresolvedMsg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnresolvedRelation other = (UnresolvedRelation) obj;
        return Objects.equals(table, other.table)
            && Objects.equals(frozen, other.frozen)
            && Objects.equals(metadataFields, other.metadataFields)
            && indexMode == other.indexMode
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }

    @Override
    public List<Object> nodeProperties() {
        return singletonList(table);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + table.index();
    }
}
