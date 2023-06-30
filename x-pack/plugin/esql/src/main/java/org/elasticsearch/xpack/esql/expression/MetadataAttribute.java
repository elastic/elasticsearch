/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Map;

public class MetadataAttribute extends TypedAttribute {

    private static final MetadataAttribute _VERSION = new MetadataAttribute(Source.EMPTY, "_version", DataTypes.LONG, true);
    private static final MetadataAttribute _INDEX = new MetadataAttribute(Source.EMPTY, "_index", DataTypes.KEYWORD, true);
    // TODO
    private static final MetadataAttribute _ID = new MetadataAttribute(Source.EMPTY, "_id", DataTypes.KEYWORD, false);

    private static final Map<String, MetadataAttribute> ATTRIBUTES_MAP = Map.of(_VERSION.name(), _VERSION, _INDEX.name(), _INDEX);

    private final boolean docValues;

    public MetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        boolean docValues
    ) {
        super(source, name, dataType, qualifier, nullability, id, synthetic);
        this.docValues = docValues;
    }

    public MetadataAttribute(Source source, String name, DataType dataType, boolean docValues) {
        this(source, name, dataType, null, Nullability.TRUE, null, false, docValues);
    }

    @Override
    protected MetadataAttribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new MetadataAttribute(source, name, type, qualifier, nullability, id, synthetic, docValues);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MetadataAttribute::new, name(), dataType(), qualifier(), nullable(), id(), synthetic(), docValues);
    }

    public boolean docValues() {
        return docValues;
    }

    private MetadataAttribute withSource(Source source) {
        return new MetadataAttribute(source, name(), dataType(), qualifier(), nullable(), id(), synthetic(), docValues());
    }

    public static MetadataAttribute create(String name, Source source) {
        MetadataAttribute attribute = ATTRIBUTES_MAP.get(name);
        return attribute != null ? attribute.withSource(source) : null;
    }

    public static DataType dataType(String name) {
        MetadataAttribute attribute = ATTRIBUTES_MAP.get(name);
        return attribute != null ? attribute.dataType() : null;
    }

    public static boolean isSupported(String name) {
        return ATTRIBUTES_MAP.containsKey(name);
    }
}
