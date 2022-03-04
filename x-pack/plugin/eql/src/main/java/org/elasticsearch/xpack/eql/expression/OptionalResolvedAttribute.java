/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

public class OptionalResolvedAttribute extends FieldAttribute {

    public OptionalResolvedAttribute(FieldAttribute fa) {
        this(fa.source(), fa.parent(), fa.name(), fa.dataType(), fa.field(), fa.qualifier(), fa.nullable(), fa.id(), fa.synthetic());
    }

    public OptionalResolvedAttribute(
        Source source,
        FieldAttribute parent,
        String name,
        DataType type,
        EsField field,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        super(source, parent, name, type, field, qualifier, nullability, id, synthetic);
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(
            this,
            OptionalResolvedAttribute::new,
            parent(),
            name(),
            dataType(),
            field(),
            qualifier(),
            nullable(),
            id(),
            synthetic()
        );
    }

    @Override
    protected Attribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        FieldAttribute qualifiedParent = parent() != null ? (FieldAttribute) parent().withQualifier(qualifier) : null;
        return new OptionalResolvedAttribute(source, qualifiedParent, name, type, field(), qualifier, nullability, id, synthetic);
    }
}
