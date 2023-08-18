/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.Metadata;

import java.util.HashMap;
import java.util.Map;

public class TestIngestCtxMetadata extends IngestDocMetadata {
    public TestIngestCtxMetadata(Map<String, Object> map, Map<String, FieldProperty<?>> properties) {
        super(map, Map.copyOf(properties), null);
    }

    public static TestIngestCtxMetadata withNullableVersion(Map<String, Object> map) {
        Map<String, FieldProperty<?>> updatedProperties = new HashMap<>(IngestDocMetadata.PROPERTIES);
        updatedProperties.replace(VERSION, new Metadata.FieldProperty<>(Number.class, true, true, FieldProperty.LONGABLE_NUMBER));
        return new TestIngestCtxMetadata(map, updatedProperties);
    }
}
