/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ActionErrorIntegrationTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean timeWarped() {
        return true; // to have control over the execution
    }

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.pluginTypes());
        types.add(ErrorActionPlugin.class);
        return Collections.unmodifiableList(types);
    }

    /**
     * This test makes sure that when an action encounters an error it should
     * not be subject to throttling. Also, the ack status of the action in the
     * watch should remain awaits_successful_execution as long as the execution
     * fails.
     */
    public void testErrorInAction() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))

                        // adding an action that throws an error and is associated with a 60 minute throttle period
                        // with such a period, on successful execution we other executions of the watch will be
                        // throttled within the hour... but on failed execution there should be no throttling
                .addAction("_action", TimeValue.timeValueMinutes(60), new ErrorAction.Builder()))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id");

        flush();

        // there should be a single history record with a failure status for the action:
        assertBusy(new Runnable() {
            @Override
            public void run() {
                long count = watchRecordCount(QueryBuilders.boolQuery()
                        .must(termsQuery("result.actions.id", "_action"))
                        .must(termsQuery("result.actions.status", "failure")));
                assertThat(count, is(1L));
            }
        });

        // now we'll trigger the watch again and make sure that it's not throttled and instead
        // writes another record to the history

        // within the 60 minute throttling period
        timeWarp().clock().fastForward(TimeValue.timeValueMinutes(randomIntBetween(1, 50)));
        timeWarp().scheduler().trigger("_id");

        flush();

        // there should be a single history record with a failure status for the action:
        assertBusy(new Runnable() {
            @Override
            public void run() {
                long count = watchRecordCount(QueryBuilders.boolQuery()
                        .must(termsQuery("result.actions.id", "_action"))
                        .must(termsQuery("result.actions.status", "failure")));
                assertThat(count, is(2L));
            }
        });

        // now lets confirm that the ack status of the action is awaits_successful_execution
        GetWatchResponse getWatchResponse = watcherClient().prepareGetWatch("_id").get();
        XContentSource watch = getWatchResponse.getSource();
        watch.getValue("status.actions._action.ack.awaits_successful_execution");
    }



    public static class ErrorActionPlugin extends Plugin {

        public ErrorActionPlugin() {
        }

        @Override
        public String name() {
            return "error-action";
        }

        @Override
        public String description() {
            return name();
        }

        public void onModule(WatcherActionModule module) {
            module.registerAction(ErrorAction.TYPE, ErrorAction.Factory.class);
        }
    }

    public static class ErrorAction implements Action {

        static final String TYPE = "error";

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

        public static class Result extends Action.Result {
            public Result() {
                super(TYPE, Status.FAILURE);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().endObject();
            }
        }

        public static class Executable extends ExecutableAction<ErrorAction> {

            public Executable(ErrorAction action, ESLogger logger) {
                super(action, logger);
            }

            @Override
            public Action.Result execute(String actionId, WatchExecutionContext context, Payload payload) throws Exception {
                throw new RuntimeException("dummy error");
            }
        }

        public static class Factory extends ActionFactory<ErrorAction, Executable> {

            @Inject
            public Factory(Settings settings) {
                super(Loggers.getLogger(Executable.class, settings));
            }

            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public ErrorAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                assert parser.nextToken() == XContentParser.Token.END_OBJECT;
                return new ErrorAction();
            }

            @Override
            public Executable createExecutable(ErrorAction action) {
                return new Executable(action, actionLogger);
            }
        }

        public static class Builder implements Action.Builder<ErrorAction> {
            @Override
            public ErrorAction build() {
                return new ErrorAction();
            }
        }
    }
}
