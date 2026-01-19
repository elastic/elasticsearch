/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions;

import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.TransportRequest;

public interface DlmStep {

    boolean stepCompleted(Index index, ProjectState projectState);

    void execute(
        Index index,
        ProjectState projectState,
        ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> transportActionsDeduplicator
    );

    String stepName();

}
