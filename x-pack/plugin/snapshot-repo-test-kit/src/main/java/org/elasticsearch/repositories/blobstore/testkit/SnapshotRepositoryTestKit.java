/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class SnapshotRepositoryTestKit extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(RepositoryAnalyzeAction.INSTANCE, RepositoryAnalyzeAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return List.of(new RestRepositoryAnalyzeAction());
    }

    static void humanReadableNanos(XContentBuilder builder, String rawFieldName, String readableFieldName, long nanos) throws IOException {
        assert rawFieldName.equals(readableFieldName) == false : rawFieldName + " vs " + readableFieldName;

        if (builder.humanReadable()) {
            builder.field(readableFieldName, TimeValue.timeValueNanos(nanos).toHumanReadableString(2));
        }

        builder.field(rawFieldName, nanos);
    }
}
