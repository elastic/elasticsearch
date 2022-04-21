/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action.operator;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestPutLifecycleAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * TODO: Add docs
 */
public class OperatorLifecycleAction extends OperatorHandler {

    public static final String KEY = "ilm";

    @Override
    public String key() {
        return KEY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<MasterNodeRequest<?>> prepare(Object source) throws IOException {
        List<MasterNodeRequest<?>> result = new ArrayList<>();

        if (source.getClass().isArray()) {
            Map<String, ?>[] sources = (Map<String, ?>[]) source;
            for (Map<String, ?> s : sources) {
                result.add(prepare(s));
            }
        } else {
            result.add(prepare((Map<String, ?>) source));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private MasterNodeRequest<?> prepare(Map<String, ?> source) throws IOException {
        String lifecycleName = (String) source.get(RestPutLifecycleAction.NAME);
        Map<String, ?> content = (Map<String, ?>) source.get(OperatorHandler.CONTENT);

        try (XContentParser parser = mapToXContentParser(content)) {
            return PutLifecycleAction.Request.parseRequest(lifecycleName, parser);
        }
    }

    @Override
    public Optional<ClusterState> transformClusterState(Collection<MasterNodeRequest<?>> requests, ClusterState previous) {
        return Optional.empty();
    }
}
