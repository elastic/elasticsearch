/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusterChangedEventUtils {
    public static List<String> indicesCreated(final ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            return Collections.emptyList();
        }
        final ClusterState state = event.state();
        final ClusterState previousState = event.previousState();
        final List<String> created = new ArrayList<>();
        for (Map.Entry<String, IndexMetadata> cursor : state.metadata().getProject().indices().entrySet()) {
            final String index = cursor.getKey();
            if (previousState.metadata().getProject().hasIndex(index)) {
                final IndexMetadata currIndexMetadata = cursor.getValue();
                final IndexMetadata prevIndexMetadata = previousState.metadata().getProject().index(index);
                if (currIndexMetadata.getIndexUUID().equals(prevIndexMetadata.getIndexUUID()) == false) {
                    created.add(index);
                }
            } else {
                created.add(index);
            }
        }
        return Collections.unmodifiableList(created);
    }
}
