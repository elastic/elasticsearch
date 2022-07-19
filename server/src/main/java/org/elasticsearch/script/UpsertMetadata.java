/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Set;

class UpsertMetadata extends UpdateMetadata {
    UpsertMetadata(String index, String id, String op, long timestamp) {
        super(
            new MetadataBuilder(4).index(index, StringField)
                .id(id, StringField)
                .op(op, StringField.withWritable().withNullable())
                .timestamp(timestamp, LongField),
            Set.of("noop", "create")
        );
    }

    @Override
    public String getRouting() {
        throw new UnsupportedOperationException("routing is unavailable for insert");
    }

    @Override
    public long getVersion() {
        throw new UnsupportedOperationException("version is unavailable for insert");
    }
}
