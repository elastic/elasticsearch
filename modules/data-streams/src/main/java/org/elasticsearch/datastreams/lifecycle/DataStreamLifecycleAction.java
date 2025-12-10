/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.Index;

import java.util.Set;

public interface DataStreamLifecycleAction {
    enum State {
        NOT_READY,
        READY,
        RUNNING,
        COMPLETE
    };

    /**
     * This takes some action on the data stream. The action is expected to be fast, or run asynchronously. It returns a set of indices
     * that ought to be ignored by subsequent actions in the current pass.
     *
     * @param projectState                    The current ProjectState
     * @param dataStream                      The data stream to be acted upon
     * @param indicesToExcludeForRemainingRun A set of indices that ought to be ignored by this action.
     */
    Set<Index> apply(
        ProjectState projectState,
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Client client,
        DataStreamLifecycleErrorStore errorStore
    );

    State getState(
        ProjectState projectState,
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Client client,
        DataStreamLifecycleErrorStore errorStore
    );
}
