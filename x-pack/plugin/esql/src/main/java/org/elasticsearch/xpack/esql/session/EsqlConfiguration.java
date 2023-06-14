/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.ql.session.Configuration;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

public class EsqlConfiguration extends Configuration implements Writeable {
    private final QueryPragmas pragmas;

    private final int resultTruncationMaxSize;

    public EsqlConfiguration(ZoneId zi, String username, String clusterName, QueryPragmas pragmas, int resultTruncationMaxSize) {
        super(zi, username, clusterName);
        this.pragmas = pragmas;
        this.resultTruncationMaxSize = resultTruncationMaxSize;
    }

    public EsqlConfiguration(StreamInput in) throws IOException {
        super(in.readZoneId(), Instant.ofEpochSecond(in.readVLong(), in.readVInt()), in.readOptionalString(), in.readOptionalString());
        this.pragmas = new QueryPragmas(in);
        this.resultTruncationMaxSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZoneId(zoneId);
        var instant = now.toInstant();
        out.writeVLong(instant.getEpochSecond());
        out.writeVInt(instant.getNano());
        out.writeOptionalString(username);
        out.writeOptionalString(clusterName);
        pragmas.writeTo(out);
        out.writeVInt(resultTruncationMaxSize);
    }

    public QueryPragmas pragmas() {
        return pragmas;
    }

    public int resultTruncationMaxSize() {
        return resultTruncationMaxSize;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            EsqlConfiguration that = (EsqlConfiguration) o;
            return resultTruncationMaxSize == that.resultTruncationMaxSize && Objects.equals(pragmas, that.pragmas);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pragmas, resultTruncationMaxSize);
    }
}
