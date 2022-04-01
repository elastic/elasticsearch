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
import java.util.Set;

/**
 * Details a potential action that a user could take to clear an issue identified by a {@link HealthService}.
 *
 * @param id A unique identifier for this kind of action
 * @param message A description of the action to be taken
 * @param affectedResources Optional list of "things" that this action should be taken on (e.g. shards, indices, or policies).
 * @param helpURL Optional evergreen url to a help document
 */
public record UserAction(String id, String message, @Nullable Set<String> affectedResources, @Nullable String helpURL)
    implements
        ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("message", message);

        if (affectedResources != null && affectedResources.size() > 0) {
            builder.field("affected_resources", affectedResources);
        }

        if (helpURL != null) {
            builder.field("help_url", helpURL);
        }

        return builder.endObject();
    }
}
