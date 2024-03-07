/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.qlcore.expression.Attribute;
import org.elasticsearch.xpack.qlcore.plan.TableIdentifier;
import org.elasticsearch.xpack.qlcore.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;

import java.util.List;

public class EsqlUnresolvedRelation extends UnresolvedRelation {

    private final List<Attribute> metadataFields;

    public EsqlUnresolvedRelation(Source source, TableIdentifier table, List<Attribute> metadataFields, String unresolvedMessage) {
        super(source, table, "", false, unresolvedMessage);
        this.metadataFields = metadataFields;
    }

    public EsqlUnresolvedRelation(Source source, TableIdentifier table, List<Attribute> metadataFields) {
        this(source, table, metadataFields, null);
    }

    public List<Attribute> metadataFields() {
        return metadataFields;
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, EsqlUnresolvedRelation::new, table(), metadataFields(), unresolvedMessage());
    }
}
