/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.pagerduty;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.notification.pagerduty.IncidentEvent;
import org.elasticsearch.xpack.watcher.notification.pagerduty.SentEvent;

import java.io.IOException;
import java.util.Objects;

public class PagerDutyAction implements Action {

    public static final String TYPE = "pagerduty";

    final IncidentEvent.Template event;

    public PagerDutyAction(IncidentEvent.Template event) {
        this.event = event;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PagerDutyAction that = (PagerDutyAction) o;
        return Objects.equals(event, that.event);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        event.toXContent(builder, params);
        return builder;
    }

    public static PagerDutyAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        IncidentEvent.Template eventTemplate = IncidentEvent.Template.parse(watchId, actionId, parser);
        return new PagerDutyAction(eventTemplate);
    }

    public static Builder builder(IncidentEvent.Template event) {
        return new Builder(new PagerDutyAction(event));
    }

    public interface Result {

        class Executed extends Action.Result implements Result {

            private final String account;
            private final SentEvent sentEvent;

            public Executed(String account, SentEvent sentEvent) {
                super(TYPE, status(sentEvent));
                this.account = account;
                this.sentEvent = sentEvent;
            }

            public SentEvent sentEvent() {
                return sentEvent;
            }

            public String account() {
                return account;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject(type);
                builder.field(XField.SENT_EVENT.getPreferredName(), sentEvent, params);
                return builder.endObject();
            }

            static Status status(SentEvent sentEvent) {
                return sentEvent.successful() ? Status.SUCCESS : Status.FAILURE;
            }
        }

        class Simulated extends Action.Result implements Result {

            private final IncidentEvent event;

            protected Simulated(IncidentEvent event) {
                super(TYPE, Status.SIMULATED);
                this.event = event;
            }

            public IncidentEvent event() {
                return event;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type)
                        .field(XField.EVENT.getPreferredName(), event, params)
                        .endObject();
            }
        }
    }

    public static class Builder implements Action.Builder<PagerDutyAction> {

        final PagerDutyAction action;

        public Builder(PagerDutyAction action) {
            this.action = action;
        }

        @Override
        public PagerDutyAction build() {
            return action;
        }
    }

    public interface XField {
        ParseField SENT_EVENT = new ParseField("sent_event");
        ParseField EVENT = new ParseField("event");
    }
}
