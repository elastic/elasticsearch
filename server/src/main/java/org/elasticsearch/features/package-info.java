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
 *         This is used to only engage new behavior when all nodes in the cluster support it.
 *     </li>
 *     <li>
 *         Ensuring nodes only join a cluster if they support all features already present on that cluster.
 *         This is to ensure that once a cluster supports a feature, it then never loses support.
 *         Conversely, when a feature is defined, it can then never be removed (but see Assumed features below).
 *     </li>
 * </ol>
 *
 * <h3>Functionality</h3>
 * This functionality starts with {@link org.elasticsearch.features.NodeFeature}. This is a single id representing
 * new or a change in functionality - exactly what that functionality is is up to the user. These are expected
 * to be {@code public static final} variables on a relevant class. Each area of code then exposes their features
 * through an implementation of {@link org.elasticsearch.features.FeatureSpecification#getFeatures}, registered as an SPI implementation.
 * <p>
 * All the features exposed by a node are included in the {@link org.elasticsearch.cluster.coordination.JoinTask.NodeJoinTask}
 * processed by {@link org.elasticsearch.cluster.coordination.NodeJoinExecutor} when a node joins a cluster. This checks
 * the joining node has all the features already present on the cluster, and then records the set of features against that node
 * in cluster state (in the {@link org.elasticsearch.cluster.ClusterFeatures} object).
 * <p>
 * Informally, the features supported by a particular node are 'node features'; when all nodes in a cluster support a particular
 * feature, that is then a 'cluster feature'.
 * <p>
 * Node features can then be checked by code to determine if all nodes in the cluster support that particular feature.
 * This is done using {@link org.elasticsearch.features.FeatureService#clusterHasFeature}. This is a fast operation - the first
 * time this method is called on a particular cluster state, the cluster features for a cluster are calculated from all the
 * node feature information, and cached in the {@link org.elasticsearch.cluster.ClusterFeatures} object.
 * Henceforth, all cluster feature checks are fast hash set lookups.
 *
 * <h3>Features test infrastructure</h3>
 * Features can be specified as conditions in YAML tests, as well as checks and conditions in code-defined rolling upgrade tests
 * (see the Elasticsearch development documentation for more information).
 * These checks are performed by the {@code TestFeatureService} interface, and its standard implementation {@code ESRestTestFeatureService}.
 *
 * <h4>Test features</h4>
 * Sometimes, you want to define a feature for nodes, but the only checks you need to do are as part of a test. In this case,
 * the feature doesn't need to be included in the production codebase, it only needs to be present for automated tests.
 * So alongside {@link org.elasticsearch.features.FeatureSpecification#getFeatures}, there is
 * {@link org.elasticsearch.features.FeatureSpecification#getTestFeatures}. This can be used to exposed node features,
 * but only for automated tests. It is ignored in production uses. This is determined by the {@link org.elasticsearch.features.FeatureData}
 * class, which uses a system property (set by the test infrastructure) to decide whether to include test features or not,
 * when gathering all the registered {@code FeatureSpecification} instances.
 *
 * <h4>Synthetic version features</h4>
 * Cluster functionality checks performed on code built from the {@code main} branch can only use features, but we also have packaged releases
 * with a longer release cadence. Sometimes tests need to be conditional on older versions (where there isn't a feature already defined
 * for you), determined after the fact. This is where synthetic version features comes in. These can be used in tests where
 * it is sensible to use a release version number (eg 8.12.3). The presence of these features is determined solely by the minimum
 * node version present in the test cluster; no actual cluster features are defined nor checked.
 * This is done by {@code ESRestTestFeatureService}, matching on features of the form {@code gte_v8.12.3}.
 * For more information on their use, see the Elasticsearch developer documentation.
 *
 * <h3>Assumed features</h3>
 * Once a feature is defined on a cluster, it can never be removed - this is to ensure that functionality that is available
 * on a cluster then never stops being available. However, this can lead to the list of features in cluster state growing ever larger.
 * It is possible to remove defined cluster features, but only on a compatibility boundary (normally a new major release).
 * To see how this can be so, it may be helpful to start with the compatibility guarantees we provide:
 * <ul>
 *     <li>
 *         The first version of a new major (eg v10.0) can only form a cluster with the highest minor
 *         of the previous major (eg v9.16).
 *     </li>
 *     <li>
 *         This means that any cluster feature that was added <em>before</em> 9.16.0 was cut will <em>always</em> be present
 *         on any cluster that has at least one v10 node in it (as we don't support mixed-version clusters of more than two versions)
 *     </li>
 *     <li>
 *         This means that the code checks for those features can be completely removed from the code in v10,
 *         and the new behavior used all the time.
 *     </li>
 *     <li>
 *         This means that the node features themselves are not required, as they are never checked in the v10 codebase.
 *     </li>
 * </ul>
 * So, starting up a fresh v10 cluster, it does not need to have any knowledge of features added before 9.16, as the cluster
 * will always be using the new functionality.
 * <p>
 *
 */
package org.elasticsearch.features;
