/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;

/**
 * ESQL sometimes can only create query plans in a certain way if all nodes in the cluster (and participating remotes)
 * are at a certain minimum transport version. This is a wrapper to carry the minimum veresion along with objects that
 * get created during planning. Where this object gets consumed, we need to assume that all nodes are at the minimum
 * transport version, so that we don't use features not supported everywhere.
 *
 * <h2>How does this work, in general?</h2>
 *
 * When an ESQL request comes in, we determine the minimum transport version of all nodes in the cluster based on the
 * {@link org.elasticsearch.cluster.ClusterState}. In case of cross-cluster search, we also get the minimum versions of
 * remote clusters from the field caps response during main index resolution. The minimum of the local and remote cluster
 * versions is the overall minimum transport version for the request.
 * <p>
 * The minimum version is available in the analyzer and optimizers and can be used to make query plans that only
 * work if all nodes are at or above a certain version. This is not required for new language features (commands,
 * functions etc.) as it's fine to fail on the transport layer when a user uses a feature that is not supported on
 * all nodes. Examples where this is required include:
 * <ul>
 *   <li>
 *       When a previously unsupported ES data type gets support: when an old node participates in the query,
 *       the type needs to be treated as unsupported to avoid serialization errors and semantically invalid results.
 *       See {@link org.elasticsearch.xpack.esql.core.type.DataType}</li>
*    <li>
 *        When an optimization relies on a feature that is only available in newer versions, but it would affect
 *        queries that are already supported on older clusters.</li>
 * </ul>
 *
 * <h2>How to make version-sensitive changes</h2>
 *
 * Let's say that we want to create an optimization for ESQL queries like
 * {@code ... | STATS ... BY field | SORT field DESC | LIMIT 10}. The optimization would fuse the commands
 * into a single {@code TopNAggregate}, which corresponds to a new operator that only keeps at most N=10 groups in memory,
 * as only the highest values for {@code field} will make it into the output.
 * <p>
 * Old nodes don't support this new {@code TopNAggregate} class, but they can already execute this query. Therefore, we
 * need to make sure that this optimization is only applied when all nodes are at or above the version that introduced
 * {@code TopNAggregate}, or we will break backward compatibility.
 * <p>
 * To achieve this, the optimizer rule can use the minimum transport version from the
 * {@link org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext}. If the minimum version is at or above the required version,
 * the optimization can be applied; otherwise, it must be skipped.
 * <p>
 * The minimum version is available throughout the planning process; it can also be used in the analyzer
 * ({@link org.elasticsearch.xpack.esql.analysis.AnalyzerContext}), or during physical planning and optimization
 * ({@link org.elasticsearch.xpack.esql.planner.mapper.Mapper}, {@link org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext}).
 *
 * <h2>TRIPLE CAUTION: Backports of version-aware query planning PRs</h2>
 *
 * Because we use a single, global minimum transport version, backporting changes that introduce a version-sensitive planning
 * change need to be handled with great care. Backporting such a change to multiple prior versions leads to situations where
 * the minimum transport version indicates that a feature is supported, but some nodes don't actually support it
 * because they're not on the latest patch version.
 * <p>
 * Let's say we introduce an optimization in 9.5.0 that requires a different query plan that older nodes
 * don't support. Backporting this to 9.4.9 is likely fine, as the minimum transport version of a mixed 9.4.x/9.5+ setup
 * (cluster or CCS) will be 9.4.x, so this optimization will be (correctly) disabled on pre-9.4.9 nodes.
 * <p>
 * However, if we also backport to 9.3.7, we have a problem: A 9.3.7/9.4.x/9.5+ setup will have a minimum transport version
 * of 9.3.7, so the optimization will be enabled even if the 9.4.x nodes don't support the new plan because they're not patched
 * to 9.4.9 yet.
 * <p>
 * Rolling upgrades from 9.3.7 to 9.4.8 would have the same problem. That's generally ok, as rolling upgrades should be performed
 * to the latest patch version of a given minor version; however, if 9.3.7 gets released before 9.4.9, users may not be
 * able to perform rolling upgrades safely at all.
 *
 */
public record Versioned<T>(T inner, TransportVersion minimumVersion) {}
