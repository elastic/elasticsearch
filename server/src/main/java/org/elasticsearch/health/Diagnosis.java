/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.health.HealthService.HEALTH_API_ID_PREFIX;

/**
 * Details a potential issue that was diagnosed by a {@link HealthService}.
 *
 * @param definition The definition of the diagnosis (e.g. message, helpURL)
 * @param affectedResources Optional list of "things" that are affected by this condition (e.g. shards, indices, or policies).
 */
public record Diagnosis(Definition definition, @Nullable List<String> affectedResources) implements ToXContentObject {

    /**
     * Details a diagnosis - cause and a potential action that a user could take to clear an issue identified by a {@link HealthService}.
     *
     * @param indicatorName The name of the health indicator service that will generate this diagnosis
     * @param id An identifier unique to this diagnosis across the health indicator that generates it
     * @param cause A description of the cause of the problem
     * @param action A description of the action to be taken to remedy the problem
     * @param helpURL Optional evergreen url to a help document
     */
    public record Definition(String indicatorName, String id, String cause, String action, String helpURL) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", HEALTH_API_ID_PREFIX + definition.indicatorName + ":diagnosis:" + definition.id);
        builder.field("cause", definition.cause);
        builder.field("action", definition.action);

        if (affectedResources != null && affectedResources.size() > 0) {
            builder.field("affected_resources", affectedResources);
        }

        builder.field("help_url", definition.helpURL);
        return builder.endObject();
    }
}
