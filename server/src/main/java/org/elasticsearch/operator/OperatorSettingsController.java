/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: Write docs
 */
public class OperatorSettingsController {
    Map<String, OperatorHandler> handlers = null;

    public void initHandlers(List<OperatorHandler> handlerList) {
        handlers = handlerList.stream().collect(Collectors.toMap(OperatorHandler::key, Function.identity()));
    }

    public ClusterState process(XContentParser parser) throws IOException {
        Map<String, Object> source = parser.map();

        source.keySet().forEach(k -> {
            OperatorHandler handler = handlers.get(k);
            if (handler == null) {
                throw new IllegalStateException("Unknown entity type " + k);
            }
            try {
                Collection<MasterNodeRequest<?>> requests = handler.prepare(source.get(k));
                for (MasterNodeRequest<?> request : requests) {
                    ActionRequestValidationException exception = request.validate();
                    if (exception != null) {
                        throw new IllegalStateException("Validation error", exception);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("Malformed input data", e);
            }
        });

        return null;
    }
}
