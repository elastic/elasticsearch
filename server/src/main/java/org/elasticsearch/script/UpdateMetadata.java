/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * The update context has read-only metadata:
 * _index, _id, _version, _routing, _type (always '_doc'), _now (timestamp in millis)
 * and read-write op that may be one of 'noop' or 'none' (legacy), 'index', 'delete' or null
 */
public class UpdateMetadata extends Metadata {
    // AbstractAsyncBulkByScrollAction.OpType uses 'noop' rather than 'none', so unify on 'noop' but allow 'none' in
    // the ctx map
    protected static final String LEGACY_NOOP_STRING = "none";

    private static final FieldProperty<String> NullStringField = StringField.withNullable();

    protected final Set<String> validOps;

    public UpdateMetadata(String index, String id, long version, String routing, String type, String op, long timestamp) {
        this(
            new MetadataBuilder(7).index(index, NullStringField)
                .id(id, NullStringField)
                .version(version, LongField)
                .routing(routing, NullStringField)
                .type(type, NullStringField)
                .op(op, NullStringField.withWritable())
                .timestamp(timestamp, LongField),
            Set.of("noop", "index", "delete")
        );
    }

    protected UpdateMetadata(MetadataBuilder builder, Set<String> validOps) {
        super(builder);
        this.validOps = validOps;
    }

    @Override
    public String getOp() {
        String op = super.getOp();
        if (LEGACY_NOOP_STRING.equals(op) || op == null || validOps.contains(op) == false) {
            // UpdateHelper.UpdateOpType.lenientFromString treats all invalid ops as "noop"
            return "noop";
        }
        return op;
    }

    @Override
    public void setOp(String op) {
        // Due to existing leniency, we cannot rely on the map validator, so we must do validation here.
        if (LEGACY_NOOP_STRING.equals(op)) {
            throw new IllegalArgumentException("'" + LEGACY_NOOP_STRING + "' is not allowed, use 'noop' instead");
        } else if (op == null || validOps.contains(op) == false) {
            throw new IllegalArgumentException(
                "op must be one of [" + validOps.stream().sorted().collect(Collectors.joining(", ")) + "], not [" + op + "]"
            );
        }
        super.setOp(op);
    }
}
