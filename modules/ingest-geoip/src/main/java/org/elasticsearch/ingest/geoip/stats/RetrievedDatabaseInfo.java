/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public record RetrievedDatabaseInfo(String name, @Nullable String md5, @Nullable Long buildDateInMillis, @Nullable String type)
    implements
        Writeable {

    public RetrievedDatabaseInfo(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString(), in.readOptionalLong(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(md5);
        out.writeOptionalLong(buildDateInMillis);
        out.writeOptionalString(type);
    }
}
