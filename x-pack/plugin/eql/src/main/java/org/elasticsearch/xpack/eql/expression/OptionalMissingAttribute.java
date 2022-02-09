/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

/**
 * Attribute replacing missing EQL optional fields.
 * They act as a named NULL expression (since literals cannot be used as keys).
 * An alias over NULL could have been used however that requires special handling for
 * field extraction.
 * To avoid potential ambiguities (an alias can wrap any type of expression) this dedicated attribute
 * is used instead that acts as a foldable.
 */
public class OptionalMissingAttribute extends Attribute {

    public OptionalMissingAttribute(Source source, String name, String qualifier) {
        super(source, name, qualifier, null);
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
        return new OptionalMissingAttribute(source, name, qualifier);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    public DataType dataType() {
        return DataTypes.NULL;
    }

    @Override
    protected NodeInfo<OptionalMissingAttribute> info() {
        return NodeInfo.create(this, OptionalMissingAttribute::new, name(), qualifier());
    }
}
