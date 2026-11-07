/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Objects;

public class LogicalOptimizerContext {
    private final Configuration configuration;
    private final FoldContext foldCtx;
    private final TransportVersion minimumVersion;
    private final EsqlFlags flags;
    /**
     * When {@code true}, a one-child VIEW {@code BranchPlan} must be kept so {@code Mapper} can mark view-branch fragments.
     */
    private final boolean requestFilterAppliesToViewOutputs;

    public LogicalOptimizerContext(
        Configuration configuration,
        FoldContext foldCtx,
        TransportVersion minimumVersion,
        EsqlFlags flags,
        boolean requestFilterAppliesToViewOutputs
    ) {
        this.configuration = configuration;
        this.foldCtx = foldCtx;
        this.minimumVersion = minimumVersion;
        this.flags = flags;
        this.requestFilterAppliesToViewOutputs = requestFilterAppliesToViewOutputs;
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

    public EsqlFlags flags() {
        return flags;
    }

    public boolean requestFilterAppliesToViewOutputs() {
        return requestFilterAppliesToViewOutputs;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LogicalOptimizerContext) obj;
        return this.configuration.equals(that.configuration)
            && this.foldCtx.equals(that.foldCtx)
            && Objects.equals(this.minimumVersion, that.minimumVersion)
            && Objects.equals(this.flags, that.flags)
            && this.requestFilterAppliesToViewOutputs == that.requestFilterAppliesToViewOutputs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration, foldCtx, minimumVersion, flags, requestFilterAppliesToViewOutputs);
    }

    @Override
    public String toString() {
        return "LogicalOptimizerContext[configuration="
            + configuration
            + ", foldCtx="
            + foldCtx
            + ", minimumVersion="
            + minimumVersion
            + ", flags="
            + flags
            + ", requestFilterAppliesToViewOutputs="
            + requestFilterAppliesToViewOutputs
            + ']';
    }

}
