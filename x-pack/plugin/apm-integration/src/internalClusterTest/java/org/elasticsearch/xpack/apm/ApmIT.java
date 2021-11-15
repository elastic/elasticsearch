/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskTracer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ApmIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), APM.class);
    }

    @TestLogging(reason="testing DEBUG logging", value="org.elasticsearch.xpack.apm:DEBUG")
    public void testModule() throws IllegalAccessException {
        List<TracingPlugin> plugins = internalCluster().getMasterNodeInstance(PluginsService.class).filterPlugins(TracingPlugin.class);
        assertThat(plugins, hasSize(1));

        TransportService transportService = internalCluster().getInstance(TransportService.class);
        final TaskTracer taskTracer = transportService.getTaskManager().getTaskTracer();
        assertThat(taskTracer, notNullValue());

        final Task testTask = new Task(randomNonNegativeLong(), "test", "action", "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());

        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "register",
                APMTracer.class.getName(),
                Level.DEBUG,
                "creating span for task [" + testTask.getId() + "]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "unregister",
                APMTracer.class.getName(),
                Level.DEBUG,
                "closing span for task [" + testTask.getId() + "]"
            )
        );

        Logger classLogger = LogManager.getLogger(APMTracer.class);
        Loggers.addAppender(classLogger, mockAppender);

        try {
            taskTracer.onTaskRegistered(testTask);
            taskTracer.onTaskUnregistered(testTask);
        } finally {
            Loggers.removeAppender(classLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }
}
