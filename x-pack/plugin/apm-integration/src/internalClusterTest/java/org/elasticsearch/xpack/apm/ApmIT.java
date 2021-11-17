/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.trace.data.SpanData;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskTracer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ApmIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), APM.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(APMTracer.APM_ENDPOINT_SETTING.getKey(), System.getProperty("tests.apm.endpoint", ""));
        secureSettings.setString(APMTracer.APM_TOKEN_SETTING.getKey(), System.getProperty("tests.apm.token", ""));
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).setSecureSettings(secureSettings).build();
    }

    @After
    public void clearRecordedSpans() {
        APMTracer.CAPTURING_SPAN_EXPORTER.clear();
    }

    public void testModule() {
        List<APM> plugins = internalCluster().getMasterNodeInstance(PluginsService.class).filterPlugins(APM.class);
        assertThat(plugins, hasSize(1));

        TransportService transportService = internalCluster().getInstance(TransportService.class);
        final TaskTracer taskTracer = transportService.getTaskManager().getTaskTracer();
        assertThat(taskTracer, notNullValue());

        final Task testTask = new Task(randomNonNegativeLong(), "test", "action", "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());

        taskTracer.onTaskRegistered(testTask);
        taskTracer.onTaskUnregistered(testTask);

        final List<SpanData> capturedSpans = APMTracer.CAPTURING_SPAN_EXPORTER.getCapturedSpans();
        boolean found = false;
        final Long targetId = testTask.getId();
        for (SpanData capturedSpan : capturedSpans) {
            if (targetId.equals(capturedSpan.getAttributes().get(AttributeKey.longKey("es.task.id")))) {
                found = true;
                assertTrue(capturedSpan.hasEnded());
            }
        }
        assertTrue(found);
    }

    public void testRecordsNestedSpans() {

        APMTracer.CAPTURING_SPAN_EXPORTER.clear();// removing start related events

        client().admin().cluster().prepareListTasks().get();

        var parentTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists").collect(toList());
        assertThat(parentTasks, hasSize(1));
        var parentTask = parentTasks.get(0);
        assertThat(parentTask.getParentSpanId(), equalTo("0000000000000000"));

        var childrenTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists[n]").collect(toList());
        assertThat(childrenTasks, hasSize(internalCluster().size()));
        for (SpanData childrenTask : childrenTasks) {
            assertThat(childrenTask.getParentSpanId(), equalTo(parentTask.getSpanId()));
            assertThat(childrenTask.getTraceId(), equalTo(parentTask.getTraceId()));
        }
    }
}
