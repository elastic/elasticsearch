/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

public class EmptyEsRelation extends EsRelation {

    public static final List<Attribute> EMPTY_OUTPUT = List.of(
        new ReferenceAttribute(Source.EMPTY, "<no-fields>", DataTypes.NULL, null, Nullability.TRUE, null, true)
    );

    public EmptyEsRelation(EsIndex index) {
        this(Source.EMPTY, index);
    }

    public EmptyEsRelation(Source source, EsIndex index) {
        super(source, index, EMPTY_OUTPUT);
        assert index.mapping().isEmpty();
    }

    @Override
    protected NodeInfo<EsRelation> info() {
        return NodeInfo.create(this, EmptyEsRelation::new, index());
    }
}
