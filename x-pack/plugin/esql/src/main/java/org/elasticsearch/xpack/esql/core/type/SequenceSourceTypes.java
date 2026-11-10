/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import java.util.Set;

/**
 * Raw mapped block types understood by the sequence event reader. Logical verification and execution share this boundary so
 * unsupported retained fields produce a query error before opening a snapshot. ES|QL's UNSUPPORTED fields remain null.
 * This does not restrict values computed by ordinary stage operators after their raw inputs have been read.
 */
public final class SequenceSourceTypes {
    private static final Set<DataType> SUPPORTED = Set.of(
        DataType.NULL,
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.COUNTER_INTEGER,
        DataType.LONG,
        DataType.COUNTER_LONG,
        DataType.UNSIGNED_LONG,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.DOUBLE,
        DataType.COUNTER_DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.IP,
        DataType.VERSION,
        DataType.SOURCE,
        DataType.FLATTENED,
        DataType.UNSUPPORTED
    );

    private SequenceSourceTypes() {}

    /** Tests raw retrieval support, including the ordinary ES|QL null representation for unsupported mappings. */
    public static boolean supports(DataType type) {
        return SUPPORTED.contains(type);
    }

    /**
     * A resolved union field requires its per-mapping conversion in addition to a mapped loader. Until sequence source
     * reads can execute that conversion with the ordinary warning budget, reject it even when its output type is primitive.
     */
    public static boolean supports(EsField field) {
        DataType type = field.getDataType();
        return supports(type)
            && (type == DataType.NULL
                || type == DataType.UNSUPPORTED
                || (field instanceof UnionTypeEsField) == false && (field instanceof UnmappedEsField) == false);
    }
}
