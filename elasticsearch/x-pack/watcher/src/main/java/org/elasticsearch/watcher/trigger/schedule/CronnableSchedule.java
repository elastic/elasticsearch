/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 *
 */
public abstract class CronnableSchedule implements Schedule {

    private static final Comparator<Cron> CRON_COMPARATOR = new Comparator<Cron>() {
        @Override
        public int compare(Cron c1, Cron c2) {
            return c1.expression().compareTo(c2.expression());
        }
    };

    protected final Cron[] crons;

    public CronnableSchedule(String... expressions) {
        this(crons(expressions));
    }

    public CronnableSchedule(Cron... crons) {
        assert crons.length > 0;
        this.crons = crons;
        Arrays.sort(crons, CRON_COMPARATOR);
    }

    @Override
    public long nextScheduledTimeAfter(long startTime, long time) {
        assert time >= startTime;
        long nextTime = Long.MAX_VALUE;
        for (Cron cron : crons) {
            nextTime = Math.min(nextTime, cron.getNextValidTimeAfter(time));
        }
        return nextTime;
    }

    public Cron[] crons() {
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

    static Cron[] crons(String... expressions) {
        Cron[] crons = new Cron[expressions.length];
        for (int i = 0; i < crons.length; i++) {
            crons[i] = new Cron(expressions[i]);
        }
        return crons;
    }
}
