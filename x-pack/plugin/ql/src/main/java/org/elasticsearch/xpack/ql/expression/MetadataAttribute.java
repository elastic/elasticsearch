/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;

public class MetadataAttribute extends TypedAttribute {

    private static final Map<String, Tuple<DataType, Boolean>> ATTRIBUTES_MAP = Map.of(
        "_version",
        tuple(DataTypes.LONG, false), // _version field is not searchable
        "_index",
        tuple(DataTypes.KEYWORD, true),
        IdFieldMapper.NAME,
        tuple(DataTypes.KEYWORD, false), // actually searchable, but fielddata access on the _id field is disallowed by default
        SourceFieldMapper.NAME,
        tuple(DataTypes.SOURCE, false)
    );

    private final boolean searchable;

    public MetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        boolean searchable
    ) {
        super(source, name, dataType, qualifier, nullability, id, synthetic);
        this.searchable = searchable;
    }

    public MetadataAttribute(Source source, String name, DataType dataType, boolean searchable) {
        this(source, name, dataType, null, Nullability.TRUE, null, false, searchable);
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
        return new MetadataAttribute(source, name, type, qualifier, nullability, id, synthetic, searchable);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MetadataAttribute::new, name(), dataType(), qualifier(), nullable(), id(), synthetic(), searchable);
    }

    public boolean searchable() {
        return searchable;
    }

    private MetadataAttribute withSource(Source source) {
        return new MetadataAttribute(source, name(), dataType(), qualifier(), nullable(), id(), synthetic(), searchable());
    }

    public static MetadataAttribute create(Source source, String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? new MetadataAttribute(source, name, t.v1(), t.v2()) : null;
    }

    public static DataType dataType(String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? t.v1() : null;
    }

    public static boolean isSupported(String name) {
        return ATTRIBUTES_MAP.containsKey(name);
    }
}
