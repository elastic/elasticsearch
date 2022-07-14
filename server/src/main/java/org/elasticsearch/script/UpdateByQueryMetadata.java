/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;

public class UpdateByQueryMetadata extends BulkMetadata {

    static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        RO_STRING,
        ID,
        RO_STRING,
        VERSION,
        RO_LONG,
        ROUTING,
        RO_NULLABLE_STRING,
        OP,
        OP_PROPERTY,
        TIMESTAMP,
        RO_LONG
    );

    public UpdateByQueryMetadata(String index, String id, long version, String routing, String op, long timestamp) {
        super(index, id, version, routing, op, timestamp, PROPERTIES);
    }
}
