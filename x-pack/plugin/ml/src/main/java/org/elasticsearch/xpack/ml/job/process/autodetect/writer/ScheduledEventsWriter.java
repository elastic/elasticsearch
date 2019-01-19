/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.EQUALS;
import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.NEW_LINE;

public class ScheduledEventsWriter {

    private static final String SCHEDULED_EVENT_PREFIX = "scheduledevent.";
    private static final String SCHEDULED_EVENT_DESCRIPTION_SUFFIX = ".description";
    private static final String RULES_SUFFIX = ".rules";

    private final Collection<ScheduledEvent> scheduledEvents;
    private final TimeValue bucketSpan;
    private final StringBuilder buffer;

    public ScheduledEventsWriter(Collection<ScheduledEvent> scheduledEvents, TimeValue bucketSpan, StringBuilder buffer) {
        this.scheduledEvents = Objects.requireNonNull(scheduledEvents);
        this.bucketSpan = Objects.requireNonNull(bucketSpan);
        this.buffer = Objects.requireNonNull(buffer);
    }

    public void write() throws IOException {
        int eventIndex = 0;
        for (ScheduledEvent event: scheduledEvents) {

            StringBuilder eventContent = new StringBuilder();
            eventContent.append(SCHEDULED_EVENT_PREFIX).append(eventIndex)
                    .append(SCHEDULED_EVENT_DESCRIPTION_SUFFIX).append(EQUALS)
                    .append(event.getDescription())
                    .append(NEW_LINE);

            eventContent.append(SCHEDULED_EVENT_PREFIX).append(eventIndex).append(RULES_SUFFIX).append(EQUALS);
            try (XContentBuilder contentBuilder = XContentFactory.jsonBuilder()) {
                contentBuilder.startArray();
                event.toDetectionRule(bucketSpan).toXContent(contentBuilder, null);
                contentBuilder.endArray();
                eventContent.append(Strings.toString(contentBuilder));
            }

            eventContent.append(NEW_LINE);
            buffer.append(eventContent.toString());

            ++eventIndex;
        }
    }
}
