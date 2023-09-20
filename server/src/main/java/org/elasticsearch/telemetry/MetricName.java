/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import java.util.Objects;

public class MetricName {
    private final String rawName;

    public MetricName(String rawName) {
        this.rawName = rawName;
    }

    public String getRawName() {
        return rawName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricName metricName = (MetricName) o;
        return Objects.equals(getRawName(), metricName.getRawName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawName);
    }

    @Override
    public String toString() {
        return "MetricName[" + rawName + "]";
    }
}
