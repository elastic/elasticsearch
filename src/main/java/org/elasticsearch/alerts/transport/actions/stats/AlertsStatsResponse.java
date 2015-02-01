/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.stats;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.alerts.AlertsBuild;
import org.elasticsearch.alerts.AlertsVersion;
import org.elasticsearch.alerts.State;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * The AlertStatsResponse response
 */
public class AlertsStatsResponse extends ActionResponse {

    private AlertsVersion version;
    private AlertsBuild build;
    private long numberOfRegisteredAlerts;
    private State alertManagerState;
    private boolean alertActionManagerStarted;
    private long alertActionManagerQueueSize;
    private long alertActionManagerLargestQueueSize;

    public AlertsStatsResponse() {
    }

    /**
     * @return The current queue size in the alert action manager
     */
    public long getAlertActionManagerQueueSize() {
        return alertActionManagerQueueSize;
    }

    void setAlertActionManagerQueueSize(long alertActionManagerQueueSize) {
        this.alertActionManagerQueueSize = alertActionManagerQueueSize;
    }

    /**
     * @return The number of alerts currently registered in the system
     */
    public long getNumberOfRegisteredAlerts() {
        return numberOfRegisteredAlerts;
    }

    void setNumberOfRegisteredAlerts(long numberOfRegisteredAlerts) {
        this.numberOfRegisteredAlerts = numberOfRegisteredAlerts;
    }

    /**
     * Returns the state of the alert manager.
     */
    public State getAlertManagerStarted() {
        return alertManagerState;
    }

    void setAlertManagerState(State alertManagerState) {
        this.alertManagerState = alertManagerState;
    }

    /**
     * @return {@code true} if the alert action manager is started
     */
    public boolean isAlertActionManagerStarted() {
        return alertActionManagerStarted;
    }

    void setAlertActionManagerStarted(boolean alertActionManagerStarted) {
        this.alertActionManagerStarted = alertActionManagerStarted;
    }

    /**
     * @return The largest queue size the alert action manager queue has grown to
     */
    public long getAlertActionManagerLargestQueueSize() {
        return alertActionManagerLargestQueueSize;
    }

    void setAlertActionManagerLargestQueueSize(long alertActionManagerLargestQueueSize) {
        this.alertActionManagerLargestQueueSize = alertActionManagerLargestQueueSize;
    }

    /**
     * @return The alerts plugin version.
     */
    public AlertsVersion getVersion() {
        return version;
    }

    void setVersion(AlertsVersion version) {
        this.version = version;
    }

    /**
     * @return The alerts plugin build information.
     */
    public AlertsBuild getBuild() {
        return build;
    }

    void setBuild(AlertsBuild build) {
        this.build = build;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numberOfRegisteredAlerts = in.readLong();
        alertActionManagerQueueSize = in.readLong();
        alertActionManagerLargestQueueSize = in.readLong();
        alertManagerState = State.fromId(in.readByte());
        alertActionManagerStarted = in.readBoolean();
        version = AlertsVersion.readVersion(in);
        build = AlertsBuild.readBuild(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(numberOfRegisteredAlerts);
        out.writeLong(alertActionManagerQueueSize);
        out.writeLong(alertActionManagerLargestQueueSize);
        out.writeByte(alertManagerState.getId());
        out.writeBoolean(alertActionManagerStarted);
        AlertsVersion.writeVersion(version, out);
        AlertsBuild.writeBuild(build, out);
    }
}
