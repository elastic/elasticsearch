/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Map;

/**
 * Marks a field mapped on a sibling subquery index but unmapped here, whose type has no implicit conversion from {@code KEYWORD} (e.g.,
 * {@code text}, {@code aggregate_metric_double}). {@code _source} only yields keyword, so the value cannot be surfaced as that type:
 * reading it fails at runtime rather than silently returning null. Without this marker the local optimizer would treat the copied
 * {@link EsField} as missing and rewrite it to null.
 */
public class PotentiallyUnmappedNonLoadableEsField extends EsField {
    private static final TransportVersion ESQL_UNMAPPED_NON_LOADABLE_ES_FIELD = TransportVersion.fromName(
        "esql_unmapped_non_loadable_es_field"
    );

    public PotentiallyUnmappedNonLoadableEsField(EsField mapped) {
        this(
            mapped.getName(),
            mapped.getDataType(),
            mapped.getProperties(),
            mapped.isAggregatable(),
            mapped.isAlias(),
            mapped.getTimeSeriesFieldType()
        );
    }

    private PotentiallyUnmappedNonLoadableEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, esDataType, properties, aggregatable, isAlias, timeSeriesFieldType);
    }

    public PotentiallyUnmappedNonLoadableEsField(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new PotentiallyUnmappedNonLoadableEsField(
            getName(),
            getDataType(),
            newProperties,
            isAggregatable(),
            isAlias(),
            getTimeSeriesFieldType()
        );
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        if (transportVersion.supports(ESQL_UNMAPPED_NON_LOADABLE_ES_FIELD)) {
            return "PotentiallyUnmappedNonLoadableEsField";
        }
        return "EsField";
    }

    @Override
    public String getNodeStringName() {
        return "PotentiallyUnmappedNonLoadableEsField";
    }
}
