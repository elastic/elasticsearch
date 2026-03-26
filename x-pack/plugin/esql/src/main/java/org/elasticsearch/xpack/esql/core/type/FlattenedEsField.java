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
import java.util.Collections;

/**
 * Marker for the root field of a flattened mapping type. The root loads the blob
 * (DataType.SOURCE). Dotted subfields (e.g. `f.x.y`) are resolved at planning time.
 *
 * This class exists solely to let the analyzer identify flattened roots unambiguously.
 */
public class FlattenedEsField extends EsField {
    public static String NAME = "FlattenedEsField";

    public FlattenedEsField(String name, boolean aggregatable) {
        super(name, DataType.SOURCE, Collections.emptyMap(), aggregatable, EsField.TimeSeriesFieldType.NONE);
    }

    public FlattenedEsField(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return NAME;
    }

    @Override
    public String getNodeStringName() {
        return NAME;
    }
}
