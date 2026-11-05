/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * A document of the {@link DownsampleStateIndex}.
 */
public sealed interface StateDoc extends ToXContentObject permits IndexTaskDoc, ShardTaskDoc {

    /**
     * The project of the operation, which is also the project of the state index holding the document.
     */
    ProjectId projectId();

    /**
     * The id of the document, unique within the state index of its project.
     */
    String id();
}
