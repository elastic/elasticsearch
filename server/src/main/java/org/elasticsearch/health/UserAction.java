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

/**
 * Details a potential action that a user could take to clear an issue identified by a {@link HealthService}.
 *
 * @param definition The definition of the user action (e.g. message, helpURL)
 * @param affectedResources Optional list of "things" that this action should be taken on (e.g. shards, indices, or policies).
 */
public record UserAction(Definition definition, @Nullable List<String> affectedResources) implements ToXContentObject {

    /**
     * Details a potential action that a user could take to clear an issue identified by a {@link HealthService}.
     *
     * @param id A unique identifier for this kind of action
     * @param message A description of the action to be taken
     * @param helpURL Optional evergreen url to a help document
     */
    public record Definition(String id, String message, @Nullable String helpURL) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("message", definition.message);

        if (affectedResources != null && affectedResources.size() > 0) {
            builder.field("affected_resources", affectedResources);
        }

        if (definition.helpURL != null) {
            builder.field("help_url", definition.helpURL);
        }

        return builder.endObject();
    }
}
