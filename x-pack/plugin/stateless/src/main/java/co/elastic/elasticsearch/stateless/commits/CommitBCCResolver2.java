/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

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
