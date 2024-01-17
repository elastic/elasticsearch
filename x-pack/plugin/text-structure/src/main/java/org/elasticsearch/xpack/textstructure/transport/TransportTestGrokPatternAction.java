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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.textstructure.action.TestGrokPatternAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportTestGrokPatternAction extends HandledTransportAction<TestGrokPatternAction.Request, TestGrokPatternAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportTestGrokPatternAction.class);

    private final ThreadPool threadPool;

    @Inject
    public TransportTestGrokPatternAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        super(
            TestGrokPatternAction.NAME,
            transportService,
            actionFilters,
            TestGrokPatternAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, TestGrokPatternAction.Request request, ActionListener<TestGrokPatternAction.Response> listener) {
        // As matching a regular expression might take a while, we run
        // in a different thread to avoid blocking the network thread.
        threadPool.generic().execute(() -> {
            try {
                listener.onResponse(getResponse(request));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private TestGrokPatternAction.Response getResponse(TestGrokPatternAction.Request request) {
        Grok grok = new Grok(GrokBuiltinPatterns.get(true), request.getGrokPattern(), logger::warn);
        List<Map<String, Object>> ranges = new ArrayList<>();
        for (String text : request.getText()) {
            ranges.add(grok.captureRanges(text));
        }
        return new TestGrokPatternAction.Response(ranges);
    }
}
