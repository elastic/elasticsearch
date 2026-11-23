/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.bytes.BytesReference;

import java.util.List;

/**
 * Serializes records into the body of one batch request of the Usage API. A stub until the change that implements the record
 * format: it throws {@link UnsupportedOperationException}, and nothing calls it while the publisher is a no-op.
 */
public final class UsageRecordJson {

    private UsageRecordJson() {}

    public static BytesReference toJson(List<UsageRecord> records) {
        throw new UnsupportedOperationException("implemented by the record branch");
    }
}
