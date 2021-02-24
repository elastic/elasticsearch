/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherMockScriptPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

/**
 * This test makes sure per-action conditions are honored.
 */
public class HistoryActionConditionTests extends AbstractWatcherIntegrationTestCase {

    private final Input input = simpleInput("key", 15).build();

    private final ExecutableCondition scriptConditionPasses = mockScriptCondition("return true;");
    private final ExecutableCondition compareConditionPasses = new CompareCondition("ctx.payload.key", CompareCondition.Op.GTE, 15);
    private final ExecutableCondition conditionPasses = randomFrom(InternalAlwaysCondition.INSTANCE,
                                                                   scriptConditionPasses, compareConditionPasses);

    private final ExecutableCondition scriptConditionFails = mockScriptCondition("return false;");
    private final ExecutableCondition compareConditionFails = new CompareCondition("ctx.payload.key", CompareCondition.Op.LT, 15);
    private final ExecutableCondition conditionFails = randomFrom(NeverCondition.INSTANCE, scriptConditionFails, compareConditionFails);

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(CustomScriptPlugin.class);
        return types;
    }

    public static class CustomScriptPlugin extends WatcherMockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("return true;", vars -> true);
            scripts.put("return false;", vars -> false);
            scripts.put("throw new IllegalStateException('failed');", vars -> {
                throw new IllegalStateException("[expected] failed hard");
            });

            return scripts;
        }

    }

    /**
     * A hard failure is where an exception is thrown by the script condition.
     */
    @SuppressWarnings("unchecked")
    public void testActionConditionWithHardFailures() throws Exception {
        final String id = "testActionConditionWithHardFailures";

        final ExecutableCondition scriptConditionFailsHard = mockScriptCondition("throw new IllegalStateException('failed');");
        final List<ExecutableCondition> actionConditionsWithFailure =
                Arrays.asList(scriptConditionFailsHard, conditionPasses, InternalAlwaysCondition.INSTANCE);

        Collections.shuffle(actionConditionsWithFailure, random());

        final int failedIndex = actionConditionsWithFailure.indexOf(scriptConditionFailsHard);

        putAndTriggerWatch(id, input, actionConditionsWithFailure.toArray(new Condition[actionConditionsWithFailure.size()]));

        flush();

        assertWatchWithMinimumActionsCount(id, ExecutionState.EXECUTED, 1);

        // only one action should have failed via condition
        final SearchResponse response = searchHistory(SearchSourceBuilder.searchSource().query(termQuery("watch_id", id)));
        assertThat(response.getHits().getTotalHits().value, is(oneOf(1L, 2L)));

        final SearchHit hit = response.getHits().getAt(0);
        final List<Object> actions = getActionsFromHit(hit.getSourceAsMap());

        for (int i = 0; i < actionConditionsWithFailure.size(); ++i) {
            final Map<String, Object> action = (Map<String, Object>)actions.get(i);
            final Map<String, Object> condition = (Map<String, Object>)action.get("condition");
            final Map<String, Object> logging = (Map<String, Object>)action.get("logging");

            assertThat(action.get("id"), is("action" + i));

            if (i == failedIndex) {
                assertThat(action.get("status"), is("condition_failed"));
                assertThat(action.get("reason"), is("condition failed. skipping: [expected] failed hard"));
                assertThat(condition, nullValue());
                assertThat(logging, nullValue());
            } else {
                assertThat(condition.get("type"), is(actionConditionsWithFailure.get(i).type()));

                assertThat(action.get("status"), is("success"));
                assertThat(condition.get("met"), is(true));
                assertThat(action.get("reason"), nullValue());
                assertThat(logging.get("logged_text"), is(Integer.toString(i)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testActionConditionWithFailures() throws Exception {
        final String id = "testActionConditionWithFailures";
        final ExecutableCondition[] actionConditionsWithFailure = new ExecutableCondition[] {
                conditionFails,
                conditionPasses,
                InternalAlwaysCondition.INSTANCE
        };
        Collections.shuffle(Arrays.asList(actionConditionsWithFailure), random());

        final int failedIndex = Arrays.asList(actionConditionsWithFailure).indexOf(conditionFails);

        putAndTriggerWatch(id, input, actionConditionsWithFailure);
        assertWatchWithMinimumActionsCount(id, ExecutionState.EXECUTED, 1);

        // only one action should have failed via condition
        final SearchResponse response = searchHistory(SearchSourceBuilder.searchSource().query(termQuery("watch_id", id)));
        assertThat(response.getHits().getTotalHits().value, is(oneOf(1L, 2L)));

        final SearchHit hit = response.getHits().getAt(0);
        final List<Object> actions = getActionsFromHit(hit.getSourceAsMap());

        for (int i = 0; i < actionConditionsWithFailure.length; ++i) {
            final Map<String, Object> action = (Map<String, Object>)actions.get(i);
            final Map<String, Object> condition = (Map<String, Object>)action.get("condition");
            final Map<String, Object> logging = (Map<String, Object>)action.get("logging");

            assertThat(action.get("id"), is("action" + i));
            assertThat(condition.get("type"), is(actionConditionsWithFailure[i].type()));

            if (i == failedIndex) {
                assertThat(action.get("status"), is("condition_failed"));
                assertThat(condition.get("met"), is(false));
                assertThat(action.get("reason"), is("condition not met. skipping"));
                assertThat(logging, nullValue());
            } else {
                assertThat(action.get("status"), is("success"));
                assertThat(condition.get("met"), is(true));
                assertThat(action.get("reason"), nullValue());
                assertThat(logging.get("logged_text"), is(Integer.toString(i)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix( bugUrl = "https://github.com/elastic/elasticsearch/issues/65064")
    public void testActionCondition() throws Exception {
        final String id = "testActionCondition";
        final List<ExecutableCondition> actionConditions = new ArrayList<>();
        //actionConditions.add(conditionPasses);
        actionConditions.add(InternalAlwaysCondition.INSTANCE);

        /*
        if (randomBoolean()) {
            actionConditions.add(InternalAlwaysCondition.INSTANCE);
        }

        Collections.shuffle(actionConditions, random());
        */

        putAndTriggerWatch(id, input, actionConditions.toArray(new Condition[actionConditions.size()]));

        flush();

        assertWatchWithMinimumActionsCount(id, ExecutionState.EXECUTED, 1);

        // all actions should be successful
        final SearchResponse response = searchHistory(SearchSourceBuilder.searchSource().query(termQuery("watch_id", id)));
        assertThat(response.getHits().getTotalHits().value, is(oneOf(1L, 2L)));

        final SearchHit hit = response.getHits().getAt(0);
        final List<Object> actions = getActionsFromHit(hit.getSourceAsMap());

        for (int i = 0; i < actionConditions.size(); ++i) {
            final Map<String, Object> action = (Map<String, Object>)actions.get(i);
            final Map<String, Object> condition = (Map<String, Object>)action.get("condition");
            final Map<String, Object> logging = (Map<String, Object>)action.get("logging");

            assertThat(action.get("id"), is("action" + i));
            assertThat(action.get("status"), is("success"));
            assertThat(condition.get("type"), is(actionConditions.get(i).type()));
            assertThat(condition.get("met"), is(true));
            assertThat(action.get("reason"), nullValue());
            assertThat(logging.get("logged_text"), is(Integer.toString(i)));
        }
    }

    /**
     * Get the "actions" from the Watch History hit.
     *
     * @param source The hit's source.
     * @return The list of "actions"
     */
    @SuppressWarnings("unchecked")
    private List<Object> getActionsFromHit(final Map<String, Object> source) {
        final Map<String, Object> result = (Map<String, Object>)source.get("result");

        return (List<Object>)result.get("actions");
    }

    /**
     * Create a Watch with the specified {@code id} and {@code input}.
     * <p>
     * The {@code actionConditions} are
     *
     * @param id The ID of the Watch
     * @param input The input to use for the Watch
     * @param actionConditions The conditions to add to the Watch
     */
    private void putAndTriggerWatch(final String id, final Input input, final Condition... actionConditions) throws Exception {
        WatchSourceBuilder source = watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(input)
                .condition(InternalAlwaysCondition.INSTANCE);

        for (int i = 0; i < actionConditions.length; ++i) {
            source.addAction("action" + i, actionConditions[i], loggingAction(Integer.toString(i)));
        }

        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), id).setSource(source).get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().trigger(id);
    }

    /**
     * Create an inline script using the {@link CustomScriptPlugin}.
     *
     * @param inlineScript The script to "compile" and run
     * @return Never {@code null}
     */
    private static ExecutableCondition mockScriptCondition(String inlineScript) {
        Script script = new Script(ScriptType.INLINE, MockScriptPlugin.NAME, inlineScript, Collections.emptyMap());
        return new ScriptCondition(script);
    }

}
