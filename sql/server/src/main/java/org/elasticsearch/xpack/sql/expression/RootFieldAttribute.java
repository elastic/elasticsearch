/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

public class RootFieldAttribute extends FieldAttribute {

    public RootFieldAttribute(Location location, String name, DataType dataType) {
        this(location, name, dataType, null, true, null, false);
    }

    public RootFieldAttribute(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        super(location, name, dataType, qualifier, nullable, id, synthetic);
    }

    @Override
    protected Expression canonicalize() {
        return new RootFieldAttribute(location(), "<none>", dataType(), null, true, id(), false);
    }

    @Override
    protected Attribute clone(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return new RootFieldAttribute(location, name, dataType, qualifier, nullable, id, synthetic);
    }

    @Override
    protected String label() {
        return "r";
    }
}