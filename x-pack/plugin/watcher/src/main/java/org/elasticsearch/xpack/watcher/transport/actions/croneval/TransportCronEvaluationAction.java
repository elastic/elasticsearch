/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.croneval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.scheduler.Cron;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationResponse;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

public class TransportCronEvaluationAction extends HandledTransportAction<CronEvaluationRequest, CronEvaluationResponse> {

    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder().appendPattern("EEE, d MMM yyyy HH:mm:ss")
        .toFormatter(Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    private final Clock clock;

    @Inject
    public TransportCronEvaluationAction(Settings settings, TransportService transportService, ActionFilters actionFilters,
                                         Clock clock) {
        super(settings, CronEvaluationAction.NAME, transportService, actionFilters,
            (Supplier<CronEvaluationRequest>) CronEvaluationRequest::new);
        this.clock = clock;
    }

    @Override
    protected void doExecute(Task task, CronEvaluationRequest request, ActionListener<CronEvaluationResponse> listener) {
        try {
            long now = Instant.now(clock).toEpochMilli();
            listener.onResponse(new CronEvaluationResponse(calculateTimestamps(now, request.getExpression(), request.getCount())));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // package private for testing
    static List<String> calculateTimestamps(long nowInMillis, String expression, int count) {
        Cron.validate(expression);
        Cron cron = new Cron(expression);

        long time = nowInMillis;
        List<String> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long prevTime = time;
            time = cron.getNextValidTimeAfter(time);
            if (time < 0) {
                String message = String.format(Locale.ROOT, "Could not compute future times since [%s], perhaps the cron expression only" +
                                               " points to times in the past?)", FORMATTER.format(Instant.ofEpochMilli(prevTime)));
                throw new IllegalArgumentException(message);
            } else {
                results.add(FORMATTER.format(Instant.ofEpochMilli(time)));
            }
        }

        return Collections.unmodifiableList(results);
    }
}
