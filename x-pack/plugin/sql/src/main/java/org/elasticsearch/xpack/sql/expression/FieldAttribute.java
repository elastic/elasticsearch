/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.Objects;

/**
 * Attribute for an ES field.
 * To differentiate between the different type of fields this class offers:
 * - name - the fully qualified name (foo.bar.tar)
 * - path - the path pointing to the field name (foo.bar)
 * - parent - the immediate parent of the field; useful for figuring out the type of field (nested vs object)
 * - nestedParent - if nested, what's the parent (which might not be the immediate one)
 */
public class FieldAttribute extends TypedAttribute {

    private final FieldAttribute parent;
    private final FieldAttribute nestedParent;
    private final String path;
    private final EsField field;

    public FieldAttribute(Location location, String name, EsField field) {
        this(location, null, name, field);
    }

    public FieldAttribute(Location location, FieldAttribute parent, String name, EsField field) {
        this(location, parent, name, field, null, true, null, false);
    }

    public FieldAttribute(Location location, FieldAttribute parent, String name, EsField field, String qualifier,
                          boolean nullable, ExpressionId id, boolean synthetic) {
        super(location, name, field.getDataType(), qualifier, nullable, id, synthetic);
        this.path = parent != null ? parent.name() : StringUtils.EMPTY;
        this.parent = parent;
        this.field = field;

        // figure out the last nested parent
        FieldAttribute nestedPar = null;
        if (parent != null) {
            nestedPar = parent.nestedParent;
            if (parent.dataType() == DataType.NESTED) {
                nestedPar = parent;
            }
        }
        this.nestedParent = nestedPar;
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(this, FieldAttribute::new, parent, name(), field, qualifier(), nullable(), id(), synthetic());
    }

    public FieldAttribute parent() {
        return parent;
    }

    public String path() {
        return path;
    }

    public String qualifiedPath() {
        // return only the qualifier is there's no path
        return qualifier() != null ? qualifier() + (Strings.hasText(path) ? "." + path : StringUtils.EMPTY) : path;
    }

    public boolean isNested() {
        return nestedParent != null;
    }

    public FieldAttribute nestedParent() {
        return nestedParent;
    }

    public boolean isInexact() {
        return field.isExact() == false;
    }

    public FieldAttribute exactAttribute() {
        if (field.isExact() == false) {
            return innerField(field.getExactField());
        }
        return this;
    }

    private FieldAttribute innerField(EsField type) {
        return new FieldAttribute(location(), this, name() + "." + type.getName(), type, qualifier(), nullable(), id(), synthetic());
    }

    @Override
    protected Expression canonicalize() {
        return new FieldAttribute(location(), null, "<none>", field, null, true, id(), false);
    }

    @Override
    protected Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        FieldAttribute qualifiedParent = parent != null ? (FieldAttribute) parent.withQualifier(qualifier) : null;
        return new FieldAttribute(location, qualifiedParent, name, field, qualifier, nullable, id, synthetic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(path, ((FieldAttribute) obj).path);
    }

    @Override
    protected String label() {
        return "f";
    }

    public EsField field() {
        return field;
    }
}
