/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import java.util.Map;

/**
 * The key of a {@code flattened} field that no index maps as a field of its own, e.g. {@code attributes.host} under a
 * {@code flattened} {@code attributes}. It is a {@code keyword} field of the index, and each shard reads it as its own mapping says.
 * <p>
 * It is a class of its own so that the analyzer can tell a key from a mapped field of the same name: with map types a key is a child
 * of its map that is not a column until a command takes it as one, while a mapped field is a column. See {@code MapKeys}. It travels
 * as a plain {@link KeywordEsField}, since only the coordinator needs to know.
 */
public class FlattenedKeyEsField extends KeywordEsField {

    public FlattenedKeyEsField(KeywordEsField key) {
        super(
            key.getName(),
            key.getDataType(),
            key.getProperties(),
            key.isAggregatable(),
            key.getPrecision(),
            key.getNormalized(),
            key.isAlias(),
            key.getTimeSeriesFieldType(),
            key.isStaticallySingleValued()
        );
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new FlattenedKeyEsField((KeywordEsField) super.withProperties(newProperties));
    }

    /**
     * Whether {@code field} is the key of a {@code flattened} field rather than a mapped field.
     */
    public static boolean isKey(EsField field) {
        return field instanceof FlattenedKeyEsField;
    }
}
