/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class UnresolvedRelation extends LeafPlan implements Unresolvable {

    private final TableIdentifier table;
    private final boolean frozen;
    private final List<Attribute> metadataFields;
    private final IndexMode indexMode;
    private final String unresolvedMsg;

    public UnresolvedRelation(
        Source source,
        TableIdentifier table,
        boolean frozen,
        List<Attribute> metadataFields,
        IndexMode indexMode,
        String unresolvedMessage
    ) {
        super(source);
        this.table = table;
        this.frozen = frozen;
        this.metadataFields = metadataFields;
        this.indexMode = indexMode;
        this.unresolvedMsg = unresolvedMessage == null ? "Unknown index [" + table.index() + "]" : unresolvedMessage;
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, UnresolvedRelation::new, table, frozen, metadataFields, indexMode, unresolvedMsg);
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
    public AttributeSet references() {
        AttributeSet refs = super.references();
        if (indexMode == IndexMode.TIME_SERIES) {
            refs = new AttributeSet(refs);
            refs.add(new UnresolvedAttribute(source(), MetadataAttribute.TIMESTAMP_FIELD));
        }
        return refs;
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
