/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.textstructure.action.TestGrokPatternAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.grok.GrokBuiltinPatterns.ECS_COMPATIBILITY_V1;

public class TransportTestGrokPatternAction extends TransportAction<TestGrokPatternAction.Request, TestGrokPatternAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportTestGrokPatternAction.class);

    @Inject
    public TransportTestGrokPatternAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        // As matching a regular expression might take a while, we run in a different thread to avoid blocking the network thread.
        super(TestGrokPatternAction.INSTANCE.name(), actionFilters, transportService.getTaskManager(), threadPool.generic());
    }

    @Override
    protected void doExecute(Task task, TestGrokPatternAction.Request request, ActionListener<TestGrokPatternAction.Response> listener) {
        listener.onResponse(getResponse(request));
    }

    private TestGrokPatternAction.Response getResponse(TestGrokPatternAction.Request request) {
        assert Transports.assertNotTransportThread("matching regexes is too expensive for a network thread");
        boolean ecsCompatibility = ECS_COMPATIBILITY_V1.equals(request.getEcsCompatibility());
        Grok grok = new Grok(GrokBuiltinPatterns.get(ecsCompatibility), request.getGrokPattern(), logger::debug);
        List<Map<String, Object>> ranges = new ArrayList<>();
        for (String text : request.getText()) {
            ranges.add(grok.captureRanges(text));
        }
        return new TestGrokPatternAction.Response(ranges);
    }
}
