/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

final class HostEventCount {
    final String hostID;
    final String stacktraceID;
    int count;

    HostEventCount(String hostID, String stacktraceID, int count) {
        this.hostID = hostID;
        this.stacktraceID = stacktraceID;
        this.count = count;
    }
}
