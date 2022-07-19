/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Set;

public class UpdateByQueryMetadata extends Metadata {
    public UpdateByQueryMetadata(String index, String id, long version, String routing, String op, long timestamp) {
        super(
            new MetadataBuilder(6).index(index, StringField)
                .id(id, StringField)
                .version(version, LongField)
                .routing(routing, StringField.withNullable())
                .op(op, WritableStringSetField(Set.of("noop", "index", "delete")))
                .timestamp(timestamp, LongField)
        );
    }
}
