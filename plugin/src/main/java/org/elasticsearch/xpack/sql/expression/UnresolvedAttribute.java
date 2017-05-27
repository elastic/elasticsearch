/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.xpack.sql.capabilities.Unresolvable;
import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import static java.lang.String.format;

public class UnresolvedAttribute extends Attribute implements Unresolvable {

    private List<String> nameParts;

    public UnresolvedAttribute(Location location, String name) {
        this(location, name, null);
    }

    public UnresolvedAttribute(Location location, String name, String qualifier) {
        super(location, name, qualifier, null);
        nameParts = Arrays.asList(name.split("\\."));
    }

    public List<String> nameParts() {
        return nameParts;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    protected Attribute clone(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return this;
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("dataType", this);
    }


    @Override
    public String nodeString() {
        return format(Locale.ROOT, "unknown column '%s'", name());
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + (qualifier() != null ? qualifier() + "." : "") + name();
    }

    @Override
    protected String label() {
        return UNRESOLVED_PREFIX;
    }
}
