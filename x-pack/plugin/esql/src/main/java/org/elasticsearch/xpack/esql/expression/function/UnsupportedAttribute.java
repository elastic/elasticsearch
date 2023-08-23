/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.util.Objects;

/**
 * Unsupported attribute meaning an attribute that has been found yet cannot be used (hence why UnresolvedAttribute
 * cannot be used) expect in special conditions (currently only in projections to allow it to flow through
 * the engine).
 * As such the field is marked as unresolved (so the verifier can pick up its usage outside project).
 */
public class UnsupportedAttribute extends FieldAttribute implements Unresolvable {

    private final String message;
    private final boolean hasCustomMessage;

    private static String errorMessage(String name, UnsupportedEsField field) {
        return "Cannot use field [" + name + "] with unsupported type [" + field.getOriginalType() + "]";
    }

    public UnsupportedAttribute(Source source, String name, UnsupportedEsField field) {
        this(source, name, field, null);
    }

    public UnsupportedAttribute(Source source, String name, UnsupportedEsField field, String customMessage) {
        this(source, name, field, customMessage, null);
    }

    public UnsupportedAttribute(Source source, String name, UnsupportedEsField field, String customMessage, NameId id) {
        super(source, null, name, field, null, Nullability.TRUE, id, false);
        this.hasCustomMessage = customMessage != null;
        this.message = customMessage == null ? errorMessage(qualifiedName(), field) : customMessage;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public UnsupportedEsField field() {
        return (UnsupportedEsField) super.field();
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(this, UnsupportedAttribute::new, name(), field(), hasCustomMessage ? message : null, id());
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
        return new UnsupportedAttribute(source, name, field(), hasCustomMessage ? message : null, id);
    }

    protected String label() {
        return "!";
    }

    @Override
    public String toString() {
        return "!" + qualifiedName();
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public String unresolvedMessage() {
        return message;
    }

    public boolean hasCustomMessage() {
        return hasCustomMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hasCustomMessage, message);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var ua = (UnsupportedAttribute) obj;
            return Objects.equals(hasCustomMessage, ua.hasCustomMessage) && Objects.equals(message, ua.message);
        }
        return false;
    }
}
