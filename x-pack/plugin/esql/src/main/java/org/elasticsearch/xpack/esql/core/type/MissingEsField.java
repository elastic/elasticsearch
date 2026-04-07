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

public class MissingEsField extends EsField {

    private static final TransportVersion ESQL_MISSING_ES_FIELD = TransportVersion.fromName("esql_missing_es_field");

    public MissingEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, esDataType, properties, aggregatable, timeSeriesFieldType);
    }

    public MissingEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, esDataType, properties, aggregatable, isAlias, timeSeriesFieldType);
    }

    public MissingEsField(StreamInput in) throws IOException {
        super(in);
    }

    public String getWriteableName(TransportVersion transportVersion) {
        if (transportVersion.supports(ESQL_MISSING_ES_FIELD)) {
            return "MissingEsField";
        }
        // This was introduced because of https://github.com/elastic/elasticsearch/issues/142968
        // Things should still work fine with nodes that don't have MissingEsField,
        // so for BWC we just fall back to EsField, which is the closest thing to MissingEsField that older nodes will understand.
        return "EsField";
    }
}
