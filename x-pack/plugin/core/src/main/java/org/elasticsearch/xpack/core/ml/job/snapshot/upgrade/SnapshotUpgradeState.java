/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.snapshot.upgrade;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum SnapshotUpgradeState implements Writeable {

    LOADING_OLD_STATE, SAVING_NEW_STATE, STOPPED, FAILED;

    public static SnapshotUpgradeState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static SnapshotUpgradeState fromStream(StreamInput in) throws IOException {
        return in.readEnum(SnapshotUpgradeState.class);
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
