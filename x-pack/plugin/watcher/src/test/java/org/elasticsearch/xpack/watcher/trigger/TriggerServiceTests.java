/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TriggerServiceTests extends ESTestCase {

    private static final String ENGINE_TYPE = "foo";
    private TriggerService service;
    private Watch watch1;
    private Watch watch2;

    @Before
    public void setupTriggerService() {
        TriggerEngine<?, ?> triggerEngine = mock(TriggerEngine.class);
        when(triggerEngine.type()).thenReturn(ENGINE_TYPE);
        service = new TriggerService(Collections.singleton(triggerEngine));

        // simple watch, input and simple action
        watch1 = createWatch("1");
        setMetadata(watch1);
        setInput(watch1);
        addAction(watch1, "my_action", null, null);

        watch2 = createWatch("2");
        setInput(watch2);
        setCondition(watch2, "script");
        addAction(watch2, "my_action", "script", null);
    }

    public void testStats() {
        service.add(watch1);

        Counters stats = service.stats();
        assertThat(stats.size(), is(20L));
        assertThat(stats.get("watch.input.none.active"), is(1L));
        assertThat(stats.get("watch.input._all.active"), is(1L));
        assertThat(stats.get("watch.condition.always.active"), is(1L));
        assertThat(stats.get("watch.condition._all.active"), is(1L));
        assertThat(stats.get("watch.action.my_action.active"), is(1L));
        assertThat(stats.get("watch.action._all.active"), is(1L));
        assertThat(stats.get("watch.metadata.active"), is(1L));
        assertThat(stats.get("watch.metadata.total"), is(1L));
        assertThat(stats.get("count.active"), is(1L));
        assertThat(stats.get("count.total"), is(1L));
        assertThat(service.count(), is(1L));

        service.add(watch2);

        stats = service.stats();
        assertThat(stats.size(), is(26L));
        assertThat(stats.get("watch.input.none.active"), is(2L));
        assertThat(stats.get("watch.input._all.active"), is(2L));
        assertThat(stats.get("watch.condition.script.active"), is(1L));
        assertThat(stats.get("watch.condition.always.active"), is(1L));
        assertThat(stats.get("watch.condition._all.active"), is(2L));
        assertThat(stats.get("watch.action.my_action.active"), is(2L));
        assertThat(stats.get("watch.action._all.active"), is(2L));
        assertThat(stats.get("watch.action.condition.script.active"), is(1L));
        assertThat(stats.get("watch.action.condition._all.active"), is(1L));
        assertThat(stats.get("watch.metadata.active"), is(1L));
        assertThat(stats.get("count.active"), is(2L));
        assertThat(service.count(), is(2L));

        service.remove("1");
        stats = service.stats();
        assertThat(stats.size(), is(22L));
        assertThat(stats.get("count.active"), is(1L));
        assertThat(service.count(), is(1L));
        assertThat(stats.get("watch.input.none.active"), is(1L));
        assertThat(stats.get("watch.input._all.active"), is(1L));
        assertThat(stats.get("watch.condition.script.active"), is(1L));
        assertThat(stats.get("watch.condition._all.active"), is(1L));
        assertThat(stats.get("watch.action.my_action.active"), is(1L));
        assertThat(stats.get("watch.action._all.active"), is(1L));
        assertThat(stats.get("watch.action.condition.script.active"), is(1L));
        assertThat(stats.get("watch.action.condition._all.active"), is(1L));

        service.remove("2");
        stats = service.stats();
        assertThat(stats.size(), is(6L));
        assertThat(stats.get("count.active"), is(0L));
        assertThat(stats.get("count.total"), is(0L));

    }

    public void testCountOnPause() {
        assertThat(service.count(), is(0L));
        service.add(watch2);
        assertThat(service.count(), is(1L));
        service.add(watch1);
        assertThat(service.count(), is(2L));
        service.pauseExecution();
        assertThat(service.count(), is(0L));
    }

    public void testCountOnStart() {
        assertThat(service.count(), is(0L));
        service.start(Arrays.asList(watch1, watch2));
        assertThat(service.count(), is(2L));
    }

    public void testCountOnStop() {
        assertThat(service.count(), is(0L));
        service.start(Arrays.asList(watch1, watch2));
        assertThat(service.count(), is(2L));
        service.stop();
        assertThat(service.count(), is(0L));
    }

    private Watch createWatch(String id) {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(id);
        Trigger trigger = mock(Trigger.class);
        when(trigger.type()).thenReturn(ENGINE_TYPE);
        when(watch.trigger()).thenReturn(trigger);
        when(watch.condition()).thenReturn(InternalAlwaysCondition.INSTANCE);
        return watch;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void setInput(Watch watch) {
        ExecutableInput noneInput = new ExecutableNoneInput();
        when(watch.input()).thenReturn(noneInput);
    }

    private void setMetadata(Watch watch) {
        Map<String, Object> metadata = Collections.singletonMap("foo", "bar");
        when(watch.metadata()).thenReturn(metadata);
    }

    private void setCondition(Watch watch, String type) {
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.type()).thenReturn(type);
        when(watch.condition()).thenReturn(condition);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void addAction(Watch watch, String type, String condition, String transform) {
        List<ActionWrapper> actions = watch.actions();
        ArrayList<ActionWrapper> newActions = new ArrayList<>(actions);
        ActionWrapper actionWrapper = mock(ActionWrapper.class);
        ExecutableAction executableAction = mock(ExecutableAction.class);
        when(executableAction.type()).thenReturn(type);
        if (condition != null) {
            ExecutableCondition executableCondition = mock(ExecutableCondition.class);
            when(executableCondition.type()).thenReturn(condition);
            when(actionWrapper.condition()).thenReturn(executableCondition);
        }
        if (transform != null) {
            ExecutableTransform executableTransform = mock(ExecutableTransform.class);
            when(executableTransform.type()).thenReturn(transform);
            when(actionWrapper.transform()).thenReturn(executableTransform);
        }
        when(actionWrapper.action()).thenReturn(executableAction);
        newActions.add(actionWrapper);
        when(watch.actions()).thenReturn(newActions);
    }
}
