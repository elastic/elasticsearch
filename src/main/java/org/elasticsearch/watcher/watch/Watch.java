/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.PeriodType;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.actions.ActionRegistry;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.ConditionRegistry;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.elasticsearch.watcher.support.secret.SensitiveXContentParser;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Watch implements TriggerEngine.Job, ToXContent {

    public static final String ALL_ACTIONS_ID = "_all";
    public static final String INCLUDE_STATUS_KEY = "include_status";

    private final String id;
    private final Trigger trigger;
    private final ExecutableInput input;
    private final ExecutableCondition condition;
    private final @Nullable ExecutableTransform transform;
    private final ExecutableActions actions;
    private final @Nullable TimeValue throttlePeriod;
    private final @Nullable Map<String, Object> metadata;
    private final WatchStatus status;

    private final transient AtomicLong nonceCounter = new AtomicLong();

    private transient long version = Versions.NOT_SET;

    public Watch(String id, Trigger trigger, ExecutableInput input, ExecutableCondition condition, @Nullable ExecutableTransform transform,
                 @Nullable TimeValue throttlePeriod, ExecutableActions actions, @Nullable Map<String, Object> metadata, WatchStatus status) {
        this.id = id;
        this.trigger = trigger;
        this.input = input;
        this.condition = condition;
        this.transform = transform;
        this.actions = actions;
        this.throttlePeriod = throttlePeriod;
        this.metadata = metadata;
        this.status = status;
    }

    public String id() {
        return id;
    }

    public Trigger trigger() {
        return trigger;
    }

    public ExecutableInput input() { return input;}

    public ExecutableCondition condition() {
        return condition;
    }

    public ExecutableTransform transform() {
        return transform;
    }

    public TimeValue throttlePeriod() {
        return throttlePeriod;
    }

    public ExecutableActions actions() {
        return actions;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public WatchStatus status() {
        return status;
    }

    public long version() {
        return version;
    }

    public void version(long version) {
        this.version = version;
    }

    /**
     * Acks this watch.
     *
     * @return  {@code true} if the status of this watch changed, {@code false} otherwise.
     */
    public boolean ack(DateTime now, String... actions) {
        return status.onAck(now, actions);
    }

    public boolean acked(String actionId) {
        ActionStatus actionStatus = status.actionStatus(actionId);
        return actionStatus.ackStatus().state() == ActionStatus.AckStatus.State.ACKED;
    }

    public long nonce() {
        return nonceCounter.getAndIncrement();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Watch watch = (Watch) o;
        return watch.id.equals(id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.TRIGGER.getPreferredName()).startObject().field(trigger.type(), trigger, params).endObject();
        builder.field(Field.INPUT.getPreferredName()).startObject().field(input.type(), input, params).endObject();
        builder.field(Field.CONDITION.getPreferredName()).startObject().field(condition.type(), condition, params).endObject();
        if (transform != null) {
            builder.field(Field.TRANSFORM.getPreferredName()).startObject().field(transform.type(), transform, params).endObject();
        }
        if (throttlePeriod != null) {
            if (builder.humanReadable()) {
                builder.field(Field.THROTTLE_PERIOD.getPreferredName(), throttlePeriod.format(PeriodType.seconds()));
            } else {
                builder.field(Field.THROTTLE_PERIOD.getPreferredName(), throttlePeriod);
            }
        }
        builder.field(Field.ACTIONS.getPreferredName(), actions, params);
        if (metadata != null) {
            builder.field(Field.METADATA.getPreferredName(), metadata);
        }
        if (params.paramAsBoolean(INCLUDE_STATUS_KEY, false)) {
            builder.field(Field.STATUS.getPreferredName(), status, params);
        }
        builder.endObject();
        return builder;
    }

    public BytesReference getAsBytes() {
        // we don't want to cache this and instead rebuild it every time on demand. The watch is in
        // memory and we don't need this redundancy
        try {
            return toXContent(jsonBuilder(), WatcherParams.builder().put(Watch.INCLUDE_STATUS_KEY, true).build()).bytes();
        } catch (IOException ioe) {
            throw new WatcherException("could not serialize watch [{}]", ioe, id);
        }
    }

    public static class Parser extends AbstractComponent {

        private final ConditionRegistry conditionRegistry;
        private final TriggerService triggerService;
        private final TransformRegistry transformRegistry;
        private final ActionRegistry actionRegistry;
        private final InputRegistry inputRegistry;
        private final SecretService secretService;
        private final ExecutableInput defaultInput;
        private final ExecutableCondition defaultCondition;
        private final ExecutableActions defaultActions;
        private final Clock clock;

        @Inject
        public Parser(Settings settings, ConditionRegistry conditionRegistry, TriggerService triggerService,
                      TransformRegistry transformRegistry, ActionRegistry actionRegistry,
                      InputRegistry inputRegistry, SecretService secretService, Clock clock) {

            super(settings);
            this.conditionRegistry = conditionRegistry;
            this.transformRegistry = transformRegistry;
            this.triggerService = triggerService;
            this.actionRegistry = actionRegistry;
            this.inputRegistry = inputRegistry;
            this.secretService = secretService;
            this.defaultInput = new ExecutableNoneInput(logger);
            this.defaultCondition = new ExecutableAlwaysCondition(logger);
            this.defaultActions = new ExecutableActions(ImmutableList.<ActionWrapper>of());
            this.clock = clock;
        }

        public Watch parse(String name, boolean includeStatus, BytesReference source) {
            return parse(name, includeStatus, false, source);
        }

        /**
         * Parses the watch represented by the given source. When parsing, any sensitive data that the
         * source might contain (e.g. passwords) will be converted to {@link org.elasticsearch.watcher.support.secret.Secret secrets}
         * Such that the returned watch will potentially hide this sensitive data behind a "secret". A secret
         * is an abstraction around sensitive data (text). There can be different implementations of how the
         * secret holds the data, depending on the wired up {@link SecretService}. When shield is installed, a
         * {@link org.elasticsearch.watcher.shield.ShieldSecretService} is used, that potentially encrypts the data
         * using Shield's configured system key.
         *
         * This method is only called once - when the user adds a new watch. From that moment on, all representations
         * of the watch in the system will be use secrets for sensitive data.
         *
         * @see org.elasticsearch.watcher.WatcherService#putWatch(String, BytesReference, TimeValue)
         */
        public Watch parseWithSecrets(String id, boolean includeStatus, BytesReference source) {
            return parse(id, includeStatus, true, source);
        }

        private Watch parse(String id, boolean includeStatus, boolean withSecrets, BytesReference source) {
            if (logger.isTraceEnabled()) {
                logger.trace("parsing watch [{}] ", source.toUtf8());
            }
            XContentParser parser = null;
            try {
                parser = XContentHelper.createParser(source);
                if (withSecrets) {
                    parser = new SensitiveXContentParser(parser, secretService);
                }
                parser.nextToken();
                return parse(id, includeStatus, parser);
            } catch (IOException ioe) {
                throw new WatcherException("could not parse watch [{}]", ioe, id);
            } finally {
                if (parser != null) {
                    parser.close();
                }
            }
        }

        public Watch parse(String id, boolean includeStatus, XContentParser parser) throws IOException {
            Trigger trigger = null;
            ExecutableInput input = defaultInput;
            ExecutableCondition condition = defaultCondition;
            ExecutableActions actions = defaultActions;
            ExecutableTransform transform = null;
            TimeValue throttlePeriod = null;
            Map<String, Object> metatdata = null;
            WatchStatus status = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == null ) {
                    throw new ParseException("could not parse watch [{}]. null token", id);
                } else if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == null || currentFieldName == null) {
                    throw new WatcherException("could not parse watch [{}], unexpected token [{}]", id, token);
                } else if (Field.TRIGGER.match(currentFieldName)) {
                    trigger = triggerService.parseTrigger(id, parser);
                } else if (Field.INPUT.match(currentFieldName)) {
                    input = inputRegistry.parse(id, parser);
                } else if (Field.CONDITION.match(currentFieldName)) {
                    condition = conditionRegistry.parseExecutable(id, parser);
                } else if (Field.TRANSFORM.match(currentFieldName)) {
                    transform = transformRegistry.parse(id, parser);
                } else if (Field.THROTTLE_PERIOD.match(currentFieldName)) {
                    try {
                        throttlePeriod = WatcherDateTimeUtils.parseTimeValue(parser, Field.THROTTLE_PERIOD.toString());
                    } catch (WatcherDateTimeUtils.ParseException pe) {
                        throw new ParseException("could not parse watch [{}]. failed to parse time value for field [{}]", pe, id, currentFieldName);
                    }
                } else if (Field.ACTIONS.match(currentFieldName)) {
                    actions = actionRegistry.parseActions(id, parser);
                } else if (Field.METADATA.match(currentFieldName)) {
                    metatdata = parser.map();
                } else if (Field.STATUS.match(currentFieldName)) {
                    if (includeStatus) {
                        status = WatchStatus.parse(id, parser);
                    } else {
                        parser.skipChildren();
                    }
                } else {
                    throw new ParseException("could not parse watch [{}]. unexpected field [{}]", id, currentFieldName);
                }
            }
            if (trigger == null) {
                throw new WatcherException("could not parse watch [{}]. missing required field [{}]", id, Field.TRIGGER.getPreferredName());
            }

            if (status != null) {
                // verify the status is valid (that every action indeed has a status)
                for (ActionWrapper action : actions) {
                    if (status.actionStatus(action.id()) == null) {
                        throw new WatcherException("could not parse watch [{}]. watch status in invalid state. action [{}] status is missing", id, action.id());
                    }
                }
            } else {
                // we need to create the initial statuses for the actions
                ImmutableMap.Builder<String, ActionStatus> actionsStatuses = ImmutableMap.builder();
                DateTime now = clock.now(DateTimeZone.UTC);
                for (ActionWrapper action : actions) {
                    actionsStatuses.put(action.id(), new ActionStatus(now));
                }
                status = new WatchStatus(actionsStatuses.build());
            }

            return new Watch(id, trigger, input, condition, transform, throttlePeriod, actions, metatdata, status);
        }
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg, Object... args) {
            super(msg, args);
        }

        public ParseException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    public interface Field {
        ParseField TRIGGER = new ParseField("trigger");
        ParseField INPUT = new ParseField("input");
        ParseField CONDITION = new ParseField("condition");
        ParseField ACTIONS = new ParseField("actions");
        ParseField TRANSFORM = new ParseField("transform");
        ParseField THROTTLE_PERIOD = new ParseField("throttle_period");
        ParseField METADATA = new ParseField("metadata");
        ParseField STATUS = new ParseField("status");
    }
}
