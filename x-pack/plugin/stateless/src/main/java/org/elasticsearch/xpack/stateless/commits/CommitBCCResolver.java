/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.Set;

public interface CommitBCCResolver {
    /**
     * Resolves the referenced BCCs used by a specific commit for the given generation.
     *
     * @param generation the generation to resolve
     * @return a set of {@link PrimaryTermAndGeneration} representing BCC dependencies for the specified commit.
     * It can return an empty set if the shard is closed or relocated as it's not expected to upload BCCs from that point on,
     * otherwise it's guaranteed to return a non-empty set since the {@code generation} must be contained in at least one BCC.
     */
    Set<PrimaryTermAndGeneration> resolveReferencedBCCsForCommit(long generation);
}
