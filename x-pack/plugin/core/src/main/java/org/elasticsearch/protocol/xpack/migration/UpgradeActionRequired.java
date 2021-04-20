/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.migration;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Indicates the type of the upgrade required for the index
 */
public enum UpgradeActionRequired implements Writeable {
    NOT_APPLICABLE,   // Indicates that the check is not applicable to this index type, the next check will be performed
    UP_TO_DATE,       // Indicates that the check finds this index to be up to date - no additional checks are required
    REINDEX,          // The index should be reindex
    UPGRADE;          // The index should go through the upgrade procedure

    public static UpgradeActionRequired fromString(String value) {
        return UpgradeActionRequired.valueOf(value.toUpperCase(Locale.ROOT));
    }

    public static UpgradeActionRequired readFromStream(StreamInput in) throws IOException {
        return in.readEnum(UpgradeActionRequired.class);
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
