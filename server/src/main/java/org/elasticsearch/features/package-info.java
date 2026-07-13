/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * The features infrastructure in Elasticsearch is responsible for two things:
 * <ol>
 *     <li>
 *         Determining when all nodes in a cluster have been upgraded to support some new functionality.
 *         This is used to only utilise new behavior when all nodes in the cluster support it.
 *     </li>
 *     <li>
 *         Ensuring nodes only join a cluster if they support all features already present on that cluster.
 *         This is to ensure that once a cluster supports a feature, it then never drops support.
 *         Conversely, when a feature is defined, it can then never be removed (but see Assumed features below).
 *     </li>
 * </ol>
 *
 * <h2>Functionality</h2>
 * This functionality starts with {@link org.elasticsearch.features.NodeFeature}. This is a single id representing
 * new or a change in functionality - exactly what functionality that feature represents is up to the developer. These are expected
 * to be {@code public static final} variables on a relevant class. Each area of code then exposes their features
 * through an implementation of {@link org.elasticsearch.features.FeatureSpecification#getFeatures}, registered as an SPI implementation.
 * <p>
 * All the features exposed by a node are included in the {@link org.elasticsearch.cluster.coordination.JoinTask.NodeJoinTask} information
 * processed by {@link org.elasticsearch.cluster.coordination.NodeJoinExecutor}, when a node attempts to join a cluster. This checks
 * the joining node has all the features already present on the cluster, and then records the set of features against that node
 * in cluster state (in the {@link org.elasticsearch.cluster.ClusterFeatures} object).
 * The calculated effective cluster features are not persisted, only the per-node feature set.
 * <p>
 * Informally, the features supported by a particular node are 'node features'; when all nodes in a cluster support a particular
 * feature, that is then a 'cluster feature'.
 * <p>
 * Node features can then be checked by code to determine if all nodes in the cluster support that particular feature.
 * This is done using {@link org.elasticsearch.features.FeatureService#clusterHasFeature}. This is a fast operation - the first
 * time this method is called on a particular cluster state, the cluster features for a cluster are calculated from all the
 * node feature information, and cached in the {@link org.elasticsearch.cluster.ClusterFeatures} object.
 * Henceforth, all cluster feature checks are fast hash set lookups, at least until the nodes or master changes.
 *
 * <h2>Features test infrastructure</h2>
 * Features can be specified as conditions in YAML tests, as well as checks and conditions in code-defined rolling upgrade tests
 * (see the Elasticsearch development documentation for more information).
 * These checks are performed by the {@code TestFeatureService} interface, and its standard implementation {@code ESRestTestFeatureService}.
 *
 * <h3>Test features</h3>
 * Sometimes, you want to define a feature for nodes, but the only checks you need to do are as part of a test. In this case,
 * the feature doesn't need to be included in the production feature set, it only needs to be present for automated tests.
 * So alongside {@link org.elasticsearch.features.FeatureSpecification#getFeatures}, there is
 * {@link org.elasticsearch.features.FeatureSpecification#getTestFeatures}. This can be used to exposed node features,
 * but only for automated tests. It is ignored in production uses. This is determined by the {@link org.elasticsearch.features.FeatureData}
 * class, which uses a system property (set by the test infrastructure) to decide whether to include test features or not,
 * when gathering all the registered {@code FeatureSpecification} instances.
 * <p>
 * Test features can be removed at-will (with appropriate backports),
 * as there is no long-term upgrade guarantees required for clusters in automated tests.
 *
 * <h3>Synthetic version features</h3>
 * Cluster functionality checks performed on code built from the {@code main} branch can only use features to check functionality,
 * but we also have branch releases with a longer release cadence. Sometimes tests need to be conditional on older versions
 * (where there isn't a feature already defined in the right place), determined some point after the release has been finalized.
 * This is where synthetic version features comes in. These can be used in tests where it is sensible to use
 * a release version number (eg 8.12.3). The presence of these features is determined solely by the minimum
 * node version present in the test cluster; no actual cluster features are defined nor checked.
 * This is done by {@code ESRestTestFeatureService}, matching on features of the form {@code gte_v8.12.3}.
 * For more information on their use, see the Elasticsearch developer documentation.
 *
 * <h2>Assumed features</h2>
 * Once a feature is defined on a cluster, it can never be removed - this is to ensure that functionality that is available
 * on a cluster then never stops being available. However, this can lead to the list of features in cluster state growing ever larger.
 * It is possible to remove defined cluster features, but only on a compatibility boundary (normally a new major release).
 * To see how this can be so, it may be helpful to start with the compatibility guarantees we provide:
 * <ul>
 *     <li>
 *         The first version of a new major (eg v9.0) can only form a cluster with the highest minor
 *         of the previous major (eg v8.18).
 *     </li>
 *     <li>
 *         This means that any cluster feature that was added <em>before</em> 8.18.0 was cut will <em>always</em> be present
 *         on any cluster that has at least one v9 node in it (as we don't support mixed-version clusters of more than two versions)
 *     </li>
 *     <li>
 *         This means that the code checks for those features can be completely removed from the code in v9,
 *         and the new behavior used all the time.
 *     </li>
 *     <li>
 *         This means that the node features themselves are not required, as they are never checked in the v9 codebase.
 *     </li>
 * </ul>
 * So, starting up a fresh v9 cluster, it does not need to have any knowledge of features added before 8.18, as the cluster
 * will always have the new functionality.
 * <p>
 * So then how do we do a rolling upgrade from 8.18 to 9.0, if features have been removed? Normally, that would prevent a 9.0
 * node from joining an 8.18 cluster, as it will not have all the required features published. However, we can make use
 * of the major version difference to allow the rolling upgrade to proceed.
 * <p>
 * This is where the {@link org.elasticsearch.features.NodeFeature#assumedAfterNextCompatibilityBoundary()} field comes in. On 8.18,
 * we can mark all the features that will be removed in 9.0 as assumed. This means that when the features infrastructure sees a
 * 9.x node, it will deem that node to have all the assumed features, even if the 9.0 node doesn't actually have those features
 * in its published set. It will allow 9.0 nodes to join the cluster missing assumed features,
 * and it will say the cluster supports a particular assumed feature even if it is missing from any 9.0 nodes in the cluster.
 * <p>
 * Essentially, 8.18 nodes (or any other version that can form a cluster with 8.x or 9.x nodes) can mediate
 * between the 8.x and 9.x feature sets, using {@code assumedAfterNextCompatibilityBoundary}
 * to mark features that have been removed from 9.x, and know that 9.x nodes still meet the requirements for those features.
 * These assumed features need to be defined before 8.18 and 9.0 are released.
 * <p>
 * To go into more detail what happens during a rolling upgrade:
 * <ol>
 *     <li>Start with a homogenous 8.18 cluster, with an 8.18 cluster feature set (including assumed features)</li>
 *     <li>
 *         The first 9.0 node joins the cluster. Even though it is missing the features marked as assumed in 8.18,
 *         the 8.18 master lets the 9.0 node join because all the missing features are marked as assumed,
 *         and it is of the next major version.
 *     </li>
 *     <li>
 *         At this point, any feature checks that happen on 8.18 nodes for assumed features pass, despite the 9.0 node
 *         not publishing those features, as the 9.0 node is assumed to meet the requirements for that feature.
 *         9.0 nodes do not have those checks at all, and the corresponding code running on 9.0 uses the new behaviour without checking.
 *     </li>
 *     <li>More 8.18 nodes get swapped for 9.0 nodes</li>
 *     <li>
 *         At some point, the master will change from an 8.18 node to a 9.0 node. The 9.0 node does not have the assumed
 *         features at all, so the new cluster feature set as calculated by the 9.0 master will only contain the features
 *         that 9.0 knows about (the calculated feature set is not persisted anywhere).
 *         The cluster has effectively dropped all the 8.18 features assumed in 9.0, whilst maintaining all behaviour.
 *         The upgrade carries on.
 *     </li>
 *     <li>
 *         If an 8.18 node were to quit and re-join the cluster still as 8.18 at this point
 *         (and there are other 8.18 nodes not yet upgraded), it will be able to join the cluster despite the master being 9.0.
 *         The 8.18 node publishes all the assumed features that 9.0 does not have - but that doesn't matter, because nodes can join
 *         with more features than are present in the cluster as a whole. The additional features are not added
 *         to the cluster feature set because not all the nodes in the cluster have those features
 *         (as there is at least one 9.0 node in the cluster - itself).
*      </li>
 *     <li>
 *         At some point, the last 8.18 node leaves the cluster, and the cluster is a homogenous 9.0 cluster
 *         with only the cluster features known about by 9.0.
 *     </li>
 * </ol>
 *
 * For any dynamic releases that occur from main, the cadence is much quicker - once a feature is present in a cluster,
 * you then only need one completed release to mark a feature as assumed, and a subsequent release to remove it from the codebase
 * and elide the corresponding check.
 */
package org.elasticsearch.features;
