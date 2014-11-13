/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.stats;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class AlertsStatsResponse extends ActionResponse {

    private long numberOfRegisteredAlerts;
    private boolean alertManagerStarted;
    private boolean alertActionManagerStarted;
    private long alertActionManagerQueueSize;

    public AlertsStatsResponse() {
    }



    public long getAlertActionManagerQueueSize() {
        return alertActionManagerQueueSize;
    }

    public void setAlertActionManagerQueueSize(long alertActionManagerQueueSize) {
        this.alertActionManagerQueueSize = alertActionManagerQueueSize;
    }

    public long getNumberOfRegisteredAlerts() {
        return numberOfRegisteredAlerts;
    }

    public void setNumberOfRegisteredAlerts(long numberOfRegisteredAlerts) {
        this.numberOfRegisteredAlerts = numberOfRegisteredAlerts;
    }

    public boolean isAlertManagerStarted() {
        return alertManagerStarted;
    }

    public void setAlertManagerStarted(boolean alertManagerStarted) {
        this.alertManagerStarted = alertManagerStarted;
    }

    public boolean isAlertActionManagerStarted() {
        return alertActionManagerStarted;
    }

    public void setAlertActionManagerStarted(boolean alertActionManagerStarted) {
        this.alertActionManagerStarted = alertActionManagerStarted;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numberOfRegisteredAlerts = in.readLong();
        alertActionManagerQueueSize = in.readLong();
        alertManagerStarted = in.readBoolean();
        alertActionManagerStarted = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(numberOfRegisteredAlerts);
        out.writeLong(alertActionManagerQueueSize);
        out.writeBoolean(alertManagerStarted);
        out.writeBoolean(alertActionManagerStarted);
    }
}
