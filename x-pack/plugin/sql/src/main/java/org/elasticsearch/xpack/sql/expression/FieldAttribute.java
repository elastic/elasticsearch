/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
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

    public FieldAttribute(Source source, String name, EsField field) {
        this(source, null, name, field);
    }

    public FieldAttribute(Source source, FieldAttribute parent, String name, EsField field) {
        this(source, parent, name, field, null, Nullability.TRUE, null, false);
    }

    public FieldAttribute(Source source, FieldAttribute parent, String name, EsField field, String qualifier,
                          Nullability nullability, ExpressionId id, boolean synthetic) {
        super(source, name, field.getDataType(), qualifier, nullability, id, synthetic);
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

    public EsField.Exact getExactInfo() {
        return field.getExactInfo();
    }

    public FieldAttribute exactAttribute() {
        EsField exactField = field.getExactField();
        if (exactField.equals(field) == false) {
            return innerField(exactField);
        }
        return this;
    }

    private FieldAttribute innerField(EsField type) {
        return new FieldAttribute(source(), this, name() + "." + type.getName(), type, qualifier(), nullable(), id(), synthetic());
    }

    @Override
    protected Expression canonicalize() {
        return new FieldAttribute(source(), null, "<none>", field, null, Nullability.TRUE, id(), false);
    }

    @Override
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability,
                              ExpressionId id, boolean synthetic) {
        FieldAttribute qualifiedParent = parent != null ? (FieldAttribute) parent.withQualifier(qualifier) : null;
        return new FieldAttribute(source, qualifiedParent, name, field, qualifier, nullability, id, synthetic);
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
