/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.scheduler.schedule;

import java.util.Arrays;
import java.util.Objects;

/**
 *
 */
public abstract class CronnableSchedule implements Schedule {

    protected final String[] crons;

    public CronnableSchedule(String... crons) {
        this.crons = crons;
        Arrays.sort(crons);
    }

    public String[] crons() {
        return crons;
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object[]) crons);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final CronnableSchedule other = (CronnableSchedule) obj;
        return Objects.deepEquals(this.crons, other.crons);
    }
}
