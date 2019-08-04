/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * This is the Snapshot Lifecycle Management (SLM) core package. This package contains the core classes for SLM, including all of the
 * custom cluster state metadata objects, execution history storage facilities, and the action definitions. For the main SLM service
 * implementation classes, please see the {@code ilm}ilm module's {@code org.elasticsearch.xpack.slm} package.
 *
 * <p>Contained within this specific package are the custom metadata objects and models used through out the SLM service. The names can
 * be confusing, so it's important to know the differences between each metadata object.
 *
 * <p>The {@link org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy} object is the user provided definition of the
 * SLM policy. This is what a user provides when creating a snapshot policy, and acts as the blueprint for the create snapshot request
 * that the service launches. It additionally surfaces the next point in time a policy should be executed.
 *
 * <p>Lateral to the policy, the {@link org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord} represents an execution
 * of a policy. It includes within it the policy name and details about its execution, success or failure.
 *
 * <p>Next is the {@link org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata} object, which not only contains
 * the {@link org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy} blueprint, but also any contextual information about
 * that policy, like the user information of who created it so that it may be used during execution, as well as the version of the policy,
 * and both the last failed and successful runs as {@link org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord}s. This
 * is the living representation of a policy within the cluster state.
 *
 * <p>When a "Get Policy" action is executed, the {@link org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem} is
 * returned instead. This is a thin wrapper around the internal
 * {@link org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata} object so that we do not expose any sensitive
 * internal headers or user information in the Get API.
 *
 * <p>Finally, the {@link org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata} class contains all living SLM
 * policies and their metadata, acting as the SLM specific root object within the cluster state.
 */
package org.elasticsearch.xpack.core.slm;
