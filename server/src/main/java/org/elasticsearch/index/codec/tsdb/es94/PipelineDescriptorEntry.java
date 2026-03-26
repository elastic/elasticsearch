/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es94;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

/**
 * Implemented by ES94 numeric entry types to provide typed access to the
 * {@link PipelineDescriptor} read from segment metadata.
 */
interface PipelineDescriptorEntry {

    PipelineDescriptor pipelineDescriptor();

    void pipelineDescriptor(PipelineDescriptor pipelineDescriptor);
}
