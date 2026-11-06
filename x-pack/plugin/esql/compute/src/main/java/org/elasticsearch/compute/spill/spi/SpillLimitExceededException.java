/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Completed into a listener, never thrown, when an append would take the backend past its cap. It names the setting
 * that holds the cap and the cap itself, so that the query's failure says disk, not memory, and says what to raise. The
 * status is "insufficient storage", so a client can tell it from a memory trip.
 */
public final class SpillLimitExceededException extends ElasticsearchException {

    private static final String SETTING_KEY = "es.spill.setting";
    private static final String LIMIT_KEY = "es.spill.limit";

    public SpillLimitExceededException(String settingName, long limitBytes, long bytesInUse, long bytesWanted) {
        super(
            "spill limit [{}] of [{}] exceeded: [{}] in use, [{}] wanted",
            ByteSizeValue.ofBytes(limitBytes),
            settingName,
            ByteSizeValue.ofBytes(bytesInUse),
            ByteSizeValue.ofBytes(bytesWanted)
        );
        addMetadata(SETTING_KEY, settingName);
        addMetadata(LIMIT_KEY, Long.toString(limitBytes));
    }

    public SpillLimitExceededException(StreamInput in) throws IOException {
        super(in);
    }

    public String settingName() {
        return getMetadata(SETTING_KEY).get(0);
    }

    public long limitBytes() {
        return Long.parseLong(getMetadata(LIMIT_KEY).get(0));
    }

    @Override
    public RestStatus status() {
        return RestStatus.INSUFFICIENT_STORAGE;
    }
}
