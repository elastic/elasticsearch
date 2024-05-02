/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.options.EsSourceOptions;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class EsqlUnresolvedRelation extends UnresolvedRelation {

    private final List<Attribute> metadataFields;
    private final EsSourceOptions esSourceOptions;

    public EsqlUnresolvedRelation(
        Source source,
        TableIdentifier table,
        List<Attribute> metadataFields,
        EsSourceOptions esSourceOptions,
        String unresolvedMessage
    ) {
        super(source, table, "", false, unresolvedMessage);
        this.metadataFields = metadataFields;
        Objects.requireNonNull(esSourceOptions);
        this.esSourceOptions = esSourceOptions;
    }

    public EsqlUnresolvedRelation(Source source, TableIdentifier table, List<Attribute> metadataFields, String unresolvedMessage) {
        this(source, table, metadataFields, EsSourceOptions.NO_OPTIONS, unresolvedMessage);
    }

    public EsqlUnresolvedRelation(Source source, TableIdentifier table, List<Attribute> metadataFields, EsSourceOptions esSourceOptions) {
        this(source, table, metadataFields, esSourceOptions, null);
    }

    public EsqlUnresolvedRelation(Source source, TableIdentifier table, List<Attribute> metadataFields) {
        this(source, table, metadataFields, EsSourceOptions.NO_OPTIONS, null);
    }

    public List<Attribute> metadataFields() {
        return metadataFields;
    }

    public EsSourceOptions esSourceOptions() {
        return esSourceOptions;
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, EsqlUnresolvedRelation::new, table(), metadataFields(), esSourceOptions(), unresolvedMessage());
    }
}
