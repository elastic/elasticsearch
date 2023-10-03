/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MappedActionFilters implements ActionFilter {

    private final Map<String, List<MappedActionFilter>> filtersByAction;

    public MappedActionFilters(List<MappedActionFilter> mappedFilters) {
        Map<String, List<MappedActionFilter>> map = new HashMap<>();
        for (var filter : mappedFilters) {
            map.computeIfAbsent(filter.actionName(), k -> new ArrayList<>()).add(filter);
        }
        map.replaceAll((k, l) -> List.copyOf(l));
        this.filtersByAction = Map.copyOf(map);
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> outerChain
    ) {
        var chain = new MappedFilterChain<>(this.filtersByAction.getOrDefault(action, List.of()), outerChain);
        chain.proceed(task, action, request, listener);
    }

    private static class MappedFilterChain<Request extends ActionRequest, Response extends ActionResponse>
        implements
            ActionFilterChain<Request, Response> {

        final List<MappedActionFilter> filters;
        final ActionFilterChain<Request, Response> outerChain;
        int index = 0;

        MappedFilterChain(List<MappedActionFilter> filters, ActionFilterChain<Request, Response> outerChain) {
            this.filters = filters;
            this.outerChain = outerChain;
        }

        @Override
        public void proceed(Task task, String action, Request request, ActionListener<Response> listener) {
            if (index < filters.size()) {
                var filter = filters.get(index++);
                filter.apply(task, action, request, listener, this);
            } else {
                outerChain.proceed(task, action, request, listener);
            }
        }
    }
}
