/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Context about the field being encoded, passed to the {@link PipelineConfigResolver}
 * for pipeline selection.
 *
 * @param blockSize the number of values per numeric block
 * @param fieldName the name of the field being encoded
 */
public record FieldContext(int blockSize, String fieldName) {}
