/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

public class EsqlUnresolvedRelation extends UnresolvedRelation {

    private final List<Attribute> metadataFields;
    private final IndexMode indexMode;

    public EsqlUnresolvedRelation(
        Source source,
        TableIdentifier table,
        List<Attribute> metadataFields,
        IndexMode indexMode,
        String unresolvedMessage
    ) {
        super(source, table, "", false, unresolvedMessage);
        this.metadataFields = metadataFields;
        this.indexMode = indexMode;
    }

    public EsqlUnresolvedRelation(Source source, TableIdentifier table, List<Attribute> metadataFields, IndexMode indexMode) {
        this(source, table, metadataFields, indexMode, null);
    }

    public List<Attribute> metadataFields() {
        return metadataFields;
    }

    public IndexMode indexMode() {
        return indexMode;
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
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, EsqlUnresolvedRelation::new, table(), metadataFields(), indexMode(), unresolvedMessage());
    }
}
