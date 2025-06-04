/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.elasticsearch.common.Strings;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;

public class TriggerService {

    private final GroupedConsumer consumer = new GroupedConsumer();
    private final Map<String, TriggerEngine<?, ?>> engines;
    private final Map<String, TriggerWatchStats> perWatchStats = new ConcurrentHashMap<>();

    public TriggerService(Set<TriggerEngine<?, ?>> engines) {
        Map<String, TriggerEngine<?, ?>> builder = new HashMap<>();
        for (TriggerEngine<?, ?> engine : engines) {
            builder.put(engine.type(), engine);
            engine.register(consumer);
        }
        this.engines = unmodifiableMap(builder);
    }

    public synchronized void start(Collection<Watch> watches) {
        for (TriggerEngine<?, ?> engine : engines.values()) {
            engine.start(watches);
        }
        watches.forEach(this::addToStats);
    }

    public synchronized void stop() {
        for (TriggerEngine<?, ?> engine : engines.values()) {
            engine.stop();
        }
        perWatchStats.clear();
    }

    /**
     * Stop execution/triggering of watches on this node, do not try to reload anything, just sit still
     */
    public synchronized void pauseExecution() {
        engines.values().forEach(TriggerEngine::pauseExecution);
        perWatchStats.clear();
    }

    /**
     * create statistics for a single watch, and store it in a local map
     * allowing for easy deletion in case the watch gets removed from the trigger service
     */
    private void addToStats(Watch watch) {
        TriggerWatchStats watchStats = TriggerWatchStats.create(watch);
        perWatchStats.put(watch.id(), watchStats);
    }

    /**
     * Returns some statistics about the watches loaded in the trigger service
     * @return a set of counters containing statistics
     */
    public Counters stats() {
        Counters counters = new Counters();
        // for bwc reasons, active/total contain the same values
        int watchCount = perWatchStats.size();
        counters.inc("count.active", watchCount);
        counters.inc("count.total", watchCount);
        counters.inc("watch.trigger._all.active", watchCount);
        counters.inc("watch.trigger._all.total", watchCount);
        counters.inc("watch.input._all.total", watchCount);
        counters.inc("watch.input._all.active", watchCount);
        perWatchStats.values().forEach(stats -> {
            if (stats.metadata) {
                counters.inc("watch.metadata.active");
                counters.inc("watch.metadata.total");
            }
            counters.inc("watch.trigger." + stats.triggerType + ".total");
            counters.inc("watch.trigger." + stats.triggerType + ".active");
            if (Strings.isNullOrEmpty(stats.scheduleType) == false) {
                counters.inc("watch.trigger.schedule." + stats.scheduleType + ".total");
                counters.inc("watch.trigger.schedule." + stats.scheduleType + ".active");
                counters.inc("watch.trigger.schedule._all.total");
                counters.inc("watch.trigger.schedule._all.active");
            }
            counters.inc("watch.input." + stats.inputType + ".active");
            counters.inc("watch.input." + stats.inputType + ".total");

            counters.inc("watch.condition." + stats.conditionType + ".active");
            counters.inc("watch.condition." + stats.conditionType + ".total");
            counters.inc("watch.condition._all.total");
            counters.inc("watch.condition._all.active");

            if (Strings.isNullOrEmpty(stats.transformType) == false) {
                counters.inc("watch.transform." + stats.transformType + ".active");
                counters.inc("watch.transform." + stats.transformType + ".total");
                counters.inc("watch.transform._all.active");
                counters.inc("watch.transform._all.total");
            }

            for (TriggerWatchStats.ActionStats action : stats.actions) {
                counters.inc("watch.action." + action.actionType + ".active");
                counters.inc("watch.action." + action.actionType + ".total");
                counters.inc("watch.action._all.active");
                counters.inc("watch.action._all.total");

                if (Strings.isNullOrEmpty(action.conditionType) == false) {
                    counters.inc("watch.action.condition." + action.conditionType + ".active");
                    counters.inc("watch.action.condition." + action.conditionType + ".total");
                    counters.inc("watch.action.condition._all.active");
                    counters.inc("watch.action.condition._all.total");
                }
                if (Strings.isNullOrEmpty(action.transformType) == false) {
                    counters.inc("watch.action.transform." + action.transformType + ".active");
                    counters.inc("watch.action.transform." + action.transformType + ".total");
                    counters.inc("watch.action.transform._all.active");
                    counters.inc("watch.action.transform._all.total");
                }
            }
        });
        return counters;
    }

    /**
     * Adds the given job to the trigger service. If there is already a registered job in this service with the
     * same job ID, the newly added job will replace the old job (the old job will not be triggered anymore)
     *
     * @param watch   The new watch
     */
    public void add(Watch watch) {
        engines.get(watch.trigger().type()).add(watch);
        addToStats(watch);
    }

    /**
     * Removes the job associated with the given name from this trigger service.
     *
     * @param jobName   The name of the job to remove
     * @return          {@code true} if the job existed and removed, {@code false} otherwise.
     */
    public boolean remove(String jobName) {
        perWatchStats.remove(jobName);
        for (TriggerEngine<?, ?> engine : engines.values()) {
            if (engine.remove(jobName)) {
                return true;
            }
        }
        return false;
    }

    public void register(Consumer<Iterable<TriggerEvent>> consumer) {
        this.consumer.add(consumer);
    }

    public TriggerEvent simulateEvent(String type, String jobId, Map<String, Object> data) {
        TriggerEngine<?, ?> engine = engines.get(type);
        if (engine == null) {
            throw illegalArgument("could not simulate trigger event. unknown trigger type [{}]", type);
        }
        return engine.simulateEvent(jobId, data, this);
    }

    public Trigger parseTrigger(String jobName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        assert token == XContentParser.Token.START_OBJECT;
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException(
                "could not parse trigger for [{}]. expected trigger type string field, but found [{}]",
                jobName,
                token
            );
        }
        String type = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "could not parse trigger [{}] for [{}]. expected trigger an object as the trigger body," + " but found [{}]",
                type,
                jobName,
                token
            );
        }
        Trigger trigger = parseTrigger(jobName, type, parser);
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException(
                "could not parse trigger [{}] for [{}]. expected [END_OBJECT] token, but found [{}]",
                type,
                jobName,
                token
            );
        }
        return trigger;
    }

    public Trigger parseTrigger(String jobName, String type, XContentParser parser) throws IOException {
        TriggerEngine<?, ?> engine = engines.get(type);
        if (engine == null) {
            throw new ElasticsearchParseException("could not parse trigger [{}] for [{}]. unknown trigger type [{}]", type, jobName, type);
        }
        return engine.parseTrigger(jobName, parser);
    }

    public TriggerEvent parseTriggerEvent(String watchId, String context, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        assert token == XContentParser.Token.START_OBJECT;
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException(
                "could not parse trigger event for [{}] for watch [{}]. expected trigger type string " + "field, but found [{}]",
                context,
                watchId,
                token
            );
        }
        String type = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "could not parse trigger event for [{}] for watch [{}]. expected trigger an object as "
                    + "the trigger body, but found [{}]",
                context,
                watchId,
                token
            );
        }
        TriggerEvent trigger = parseTriggerEvent(watchId, context, type, parser);
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException(
                "could not parse trigger [{}] for [{}]. expected [END_OBJECT] token, but found [{}]",
                type,
                context,
                token
            );
        }
        return trigger;
    }

    public TriggerEvent parseTriggerEvent(String watchId, String context, String type, XContentParser parser) throws IOException {
        TriggerEngine<?, ?> engine = engines.get(type);
        if (engine == null) {
            throw new ElasticsearchParseException("Unknown trigger type [{}]", type);
        }
        return engine.parseTriggerEvent(this, watchId, context, parser);
    }

    public long count() {
        return perWatchStats.size();
    }

    static class GroupedConsumer implements java.util.function.Consumer<Iterable<TriggerEvent>> {

        private List<Consumer<Iterable<TriggerEvent>>> consumers = new CopyOnWriteArrayList<>();

        public void add(Consumer<Iterable<TriggerEvent>> consumer) {
            consumers.add(consumer);
        }

        @Override
        public void accept(Iterable<TriggerEvent> events) {
            consumers.forEach(c -> c.accept(events));
        }
    }
}
