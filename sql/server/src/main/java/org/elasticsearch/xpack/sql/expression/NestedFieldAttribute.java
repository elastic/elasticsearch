/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.List;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import static java.util.Collections.emptyList;

public class NestedFieldAttribute extends FieldAttribute {

    private final List<String> parents;
    private final String parentPath;
    
    public NestedFieldAttribute(Location location, String name, DataType dataType, List<String> parents) {
        this(location, name, dataType, null, true, null, false, parents);
    }

    public NestedFieldAttribute(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic, List<String> parents) {
        super(location, name, dataType, qualifier, nullable, id, synthetic);
        this.parents = parents == null || parents.isEmpty() ? emptyList() : parents;
        this.parentPath = StringUtils.concatWithDot(parents);
    }
    
    public List<String> parents() {
        return parents;
    }

    public String parentPath() {
        return parentPath;
    }

    @Override
    protected Expression canonicalize() {
        return new NestedFieldAttribute(location(), "<none>", dataType(), null, true, id(), false, emptyList());
    }

    @Override
    protected Attribute clone(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return new NestedFieldAttribute(location, name, dataType, qualifier, nullable, id, synthetic, parents);
    }

    @Override
    public String toString() {
        if (parents.size() > 0) {
            return name().replace('.', '>') + "#" + id();
        }
        return super.toString(); 
    }

    @Override
    protected String label() {
        return "n";
    }
}