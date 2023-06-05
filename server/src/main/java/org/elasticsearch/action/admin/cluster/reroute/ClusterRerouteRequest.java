/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to submit cluster reroute allocation commands
 */
public class ClusterRerouteRequest extends AcknowledgedRequest<ClusterRerouteRequest> {
    private AllocationCommands commands = new AllocationCommands();
    private boolean dryRun;
    private boolean explain;
    private boolean retryFailed;

    public ClusterRerouteRequest(StreamInput in) throws IOException {
        super(in);
        commands = AllocationCommands.readFrom(in);
        dryRun = in.readBoolean();
        explain = in.readBoolean();
        retryFailed = in.readBoolean();
    }

    public ClusterRerouteRequest() {}

    /**
     * Adds allocation commands to be applied to the cluster. Note, can be empty, in which case
     * will simply run a simple "reroute".
     */
    public ClusterRerouteRequest add(AllocationCommand... commands) {
        this.commands.add(commands);
        return this;
    }

    /**
     * Sets a dry run flag (defaults to {@code false}) allowing to run the commands without
     * actually applying them to the cluster state.
     */
    public ClusterRerouteRequest dryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    /**
     * Returns the current dry run flag which allows to run the commands without actually applying them.
     */
    public boolean dryRun() {
        return this.dryRun;
    }

    /**
     * Sets the explain flag, which will collect information about the reroute
     * request without executing the actions. Similar to dryRun,
     * but human-readable.
     */
    public ClusterRerouteRequest explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Sets the retry failed flag (defaults to {@code false}). If true, the
     * request will retry allocating shards that can't currently be allocated due to too many allocation failures.
     */
    public ClusterRerouteRequest setRetryFailed(boolean retryFailed) {
        this.retryFailed = retryFailed;
        return this;
    }

    /**
     * Returns the current explain flag
     */
    public boolean explain() {
        return this.explain;
    }

    /**
     * Returns the current retry failed flag
     */
    public boolean isRetryFailed() {
        return this.retryFailed;
    }

    /**
     * Set the allocation commands to execute.
     */
    public ClusterRerouteRequest commands(AllocationCommands commands) {
        this.commands = commands;
        return this;
    }

    /**
     * Returns the allocation commands to execute
     */
    public AllocationCommands getCommands() {
        return commands;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        AllocationCommands.writeTo(commands, out);
        out.writeBoolean(dryRun);
        out.writeBoolean(explain);
        out.writeBoolean(retryFailed);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClusterRerouteRequest other = (ClusterRerouteRequest) obj;
        // Override equals and hashCode for testing
        return Objects.equals(commands, other.commands)
            && Objects.equals(dryRun, other.dryRun)
            && Objects.equals(explain, other.explain)
            && Objects.equals(timeout, other.timeout)
            && Objects.equals(retryFailed, other.retryFailed)
            && Objects.equals(masterNodeTimeout, other.masterNodeTimeout);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(commands, dryRun, explain, timeout, retryFailed, masterNodeTimeout);
    }
}
