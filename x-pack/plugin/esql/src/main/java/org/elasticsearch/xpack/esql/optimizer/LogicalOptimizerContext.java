/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Objects;

public class LogicalOptimizerContext {
    private final Configuration configuration;
    private final FoldContext foldCtx;
    private final TransportVersion minimumVersion;
    @Nullable
    private final TimestampBounds timestampBounds;

    public LogicalOptimizerContext(Configuration configuration, FoldContext foldCtx, TransportVersion minimumVersion) {
        this(configuration, foldCtx, minimumVersion, null);
    }

    public LogicalOptimizerContext(
        Configuration configuration,
        FoldContext foldCtx,
        TransportVersion minimumVersion,
        @Nullable TimestampBounds timestampBounds
    ) {
        this.configuration = configuration;
        this.foldCtx = foldCtx;
        this.minimumVersion = minimumVersion;
        this.timestampBounds = timestampBounds;
    }

    public Configuration configuration() {
        return configuration;
    }

    public FoldContext foldCtx() {
        return foldCtx;
    }

    public TransportVersion minimumVersion() {
        return minimumVersion;
    }

    @Nullable
    public TimestampBounds timestampBounds() {
        return timestampBounds;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LogicalOptimizerContext) obj;
        return this.configuration.equals(that.configuration)
            && this.foldCtx.equals(that.foldCtx)
            && Objects.equals(this.minimumVersion, that.minimumVersion)
            && Objects.equals(this.timestampBounds, that.timestampBounds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration, foldCtx, minimumVersion, timestampBounds);
    }

    @Override
    public String toString() {
        return "LogicalOptimizerContext[configuration="
            + configuration
            + ", foldCtx="
            + foldCtx
            + ", minimumVersion="
            + minimumVersion
            + ", timestampBounds="
            + timestampBounds
            + ']';
    }

}
