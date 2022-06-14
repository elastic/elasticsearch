/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor;

/**
 * Class that represents the Health status for a node as determined by {@link NodeHealthService} and provides additional
 * info explaining the reasons
 */
public record StatusInfo(Status status, String info) {

    public enum Status {
        HEALTHY,
        UNHEALTHY
    }

    public String getInfo() {
        return info;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "status[" + status + "]" + ", info[" + info + "]";
    }
}
