/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.execution.ManualExecutionContext;
import org.elasticsearch.watcher.execution.ManualExecutionTests.ExecutionRunner;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;


/** 
 * Two groovy-using methods from ManualExecutionTests.
 * They appear to be using groovy as a way to sleep.
 */
public class GroovyManualExecutionIT extends AbstractWatcherIntegrationTestCase {
  
    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(GroovyPlugin.class);
        return types;
    }
  
    @Override
    protected boolean enableShield() {
        return false;
    }
    
    public void testWatchExecutionDuration() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition((new Script.Builder.Inline("sleep 100; return true")).build()))
                .addAction("log", loggingAction("foobar"));

        Watch watch = watchParser().parse("_id", false, watchBuilder.buildAsBytes(XContentType.JSON));
        ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(watch, false, new ManualTriggerEvent("_id",
                new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC))),
                new TimeValue(1, TimeUnit.HOURS));
        WatchRecord record = executionService().execute(ctxBuilder.build());
        assertThat(record.result().executionDurationMs(), greaterThanOrEqualTo(100L));
    }

    public void testForceDeletionOfLongRunningWatch() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition((new Script.Builder.Inline("sleep 10000; return true")).build()))
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));

        int numberOfThreads = scaledRandomIntBetween(1, 5);
        PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();
        assertThat(putWatchResponse.getVersion(), greaterThan(0L));
        refresh();
        assertThat(watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().isFound(), equalTo(true));

        CountDownLatch startLatch = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; ++i) {
            threads.add(new Thread(new ExecutionRunner(watchService(), executionService(), "_id", startLatch)));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        DeleteWatchResponse deleteWatchResponse = watcherClient().prepareDeleteWatch("_id").setForce(true).get();
        assertThat(deleteWatchResponse.isFound(), is(true));

        deleteWatchResponse = watcherClient().prepareDeleteWatch("_id").get();
        assertThat(deleteWatchResponse.isFound(), is(false));

        startLatch.countDown();

        long startJoin = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.join();
        }
        long endJoin = System.currentTimeMillis();
        TimeValue tv = new TimeValue(10 * (numberOfThreads+1), TimeUnit.SECONDS);
        assertThat("Shouldn't take longer than [" + tv.getSeconds() + "] seconds for all the threads to stop", (endJoin - startJoin),
                lessThan(tv.getMillis()));
    }
    
}
