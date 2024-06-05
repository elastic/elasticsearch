/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * The status of categorization for a job. OK is default, WARN
 * means that inappropriate numbers of categories are being found
 */
public enum CategorizationStatus implements Writeable {
    OK,
    WARN;

    public static CategorizationStatus fromString(String statusName) {
        return valueOf(statusName.trim().toUpperCase(Locale.ROOT));
    }

    public static CategorizationStatus readFromStream(StreamInput in) throws IOException {
        return in.readEnum(CategorizationStatus.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
