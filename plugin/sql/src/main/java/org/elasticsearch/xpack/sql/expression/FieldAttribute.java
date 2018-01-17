/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.analysis.index.MappingException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.KeywordType;
import org.elasticsearch.xpack.sql.type.NestedType;
import org.elasticsearch.xpack.sql.type.StringType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.Map;
import java.util.Map.Entry;
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

    public FieldAttribute(Location location, String name, DataType dataType) {
        this(location, null, name, dataType);
    }

    public FieldAttribute(Location location, FieldAttribute parent, String name, DataType dataType) {
        this(location, parent, name, dataType, null, true, null, false);
    }

    public FieldAttribute(Location location, FieldAttribute parent, String name, DataType dataType, String qualifier,
            boolean nullable, ExpressionId id, boolean synthetic) {
        super(location, name, dataType, qualifier, nullable, id, synthetic);
        this.path = parent != null ? parent.name() : StringUtils.EMPTY;
        this.parent = parent;

        // figure out the last nested parent
        FieldAttribute nestedPar = null;
        if (parent != null) {
            nestedPar = parent.nestedParent;
            if (parent.dataType() instanceof NestedType) {
                nestedPar = parent;
            }
        }
        this.nestedParent = nestedPar;
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(this, FieldAttribute::new,
            parent, name(), dataType(), qualifier(), nullable(), id(), synthetic());
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
        return (dataType() instanceof StringType && ((StringType) dataType()).isInexact());
    }

    public FieldAttribute exactAttribute() {
        if (isInexact()) {
            Map<String, KeywordType> exactFields = ((StringType) dataType()).exactKeywords();
            if (exactFields.size() == 1) {
                Entry<String, KeywordType> entry = exactFields.entrySet().iterator().next();
                return innerField(entry.getKey(), entry.getValue());
            }
            if (exactFields.isEmpty()) {
                throw new MappingException(
                        "No keyword/multi-field defined exact matches for [%s]; define one or use MATCH/QUERY instead",
                        name());
            }
            // pick the default - keyword
            if (exactFields.size() > 1) {
                throw new MappingException("Multiple exact keyword candidates %s available for [%s]; specify which one to use",
                        exactFields.keySet(), name());
            }
        }
        return this;
    }

    private FieldAttribute innerField(String subFieldName, DataType type) {
        return new FieldAttribute(location(), this, name() + "." + subFieldName, type, qualifier(), nullable(), id(), synthetic());
    }

    @Override
    protected Expression canonicalize() {
        return new FieldAttribute(location(), null, "<none>", dataType(), null, true, id(), false);
    }

    @Override
    protected Attribute clone(Location location, String name, DataType dataType, String qualifier, boolean nullable,
            ExpressionId id, boolean synthetic) {
        FieldAttribute qualifiedParent = parent != null ? (FieldAttribute) parent.withQualifier(qualifier) : null;
        return new FieldAttribute(location, qualifiedParent, name, dataType, qualifier, nullable, id, synthetic);
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
}
