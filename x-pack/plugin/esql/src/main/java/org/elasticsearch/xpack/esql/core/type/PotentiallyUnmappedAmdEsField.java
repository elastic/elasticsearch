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
 * Marks an {@code aggregate_metric_double} field that is mapped on a sibling subquery
 * index and should be loaded from {@code _source} here. Without this marker the local
 * optimizer treats a copied AMD {@link EsField} as missing and rewrites it to null.
 */
public class PotentiallyUnmappedAmdEsField extends EsField {
    private static final TransportVersion ESQL_UNMAPPED_AMD_ES_FIELD = TransportVersion.fromName("esql_unmapped_amd_es_field");

    public PotentiallyUnmappedAmdEsField(EsField mapped) {
        this(mapped.getName(), mapped.getProperties(), mapped.isAggregatable(), mapped.isAlias(), mapped.getTimeSeriesFieldType());
    }

    private PotentiallyUnmappedAmdEsField(
        String name,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, DataType.AGGREGATE_METRIC_DOUBLE, properties, aggregatable, isAlias, timeSeriesFieldType);
    }

    public PotentiallyUnmappedAmdEsField(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new PotentiallyUnmappedAmdEsField(getName(), newProperties, isAggregatable(), isAlias(), getTimeSeriesFieldType());
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        if (transportVersion.supports(ESQL_UNMAPPED_AMD_ES_FIELD)) {
            return "PotentiallyUnmappedAmdEsField";
        }
        return "EsField";
    }

    @Override
    public String getNodeStringName() {
        return "PotentiallyUnmappedAmdEsField";
    }
}
