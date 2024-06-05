/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.xpack.core.scheduler.Cron;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public abstract class CronnableSchedule implements Schedule {

    private static final Comparator<Cron> CRON_COMPARATOR = Comparator.comparing(Cron::expression);

    protected final Cron[] crons;

    CronnableSchedule(String... expressions) {
        this(crons(expressions));
    }

    private CronnableSchedule(Cron... crons) {
        assert crons.length > 0;
        this.crons = crons;
        Arrays.sort(crons, CRON_COMPARATOR);
    }

    @Override
    public long nextScheduledTimeAfter(long startTime, long time) {
        assert time >= startTime;
        return Arrays.stream(crons)
            .map(cron -> cron.getNextValidTimeAfter(time))
            // filter out expired dates before sorting
            .filter(nextValidTime -> nextValidTime > -1)
            .sorted()
            .findFirst()
            // no date in the future found, return -1 to the caller
            .orElse(-1L);
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
