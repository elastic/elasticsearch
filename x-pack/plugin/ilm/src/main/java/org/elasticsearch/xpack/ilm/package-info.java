/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * This is the Index Lifecycle Management (ILM) main package.
 *
 * The ILM entry point is {@link org.elasticsearch.xpack.ilm.IndexLifecycleService} which calls into
 * {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner}.
 *
 * The {@link org.elasticsearch.xpack.ilm.IndexLifecycleService} goes through the indices that have ILM policies configured, retrieves
 * the current execution {@link org.elasticsearch.xpack.core.ilm.Step.StepKey} from the index's
 * {@link org.elasticsearch.xpack.core.ilm.LifecycleExecutionState} and dispatches the step execution to the appropriate
 * {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner} method.
 * This happens in:
 * <ul>
 *  <li>{@link org.elasticsearch.xpack.ilm.IndexLifecycleService#onMaster()} which runs when a master is elected (first election when the
 *      cluster starts up or due to the previous master having stepped down) and executes only
 *      {@link org.elasticsearch.xpack.core.ilm.AsyncActionStep}s
 *  </li>
 *  <li>
 *      {@link org.elasticsearch.xpack.ilm.IndexLifecycleService#triggerPolicies(org.elasticsearch.cluster.ClusterState, boolean)}
 *      which serves 2 purposes:
 *      <ul>
 *          <li>
 *              Run policy steps that need to be triggered as a result of a cluster change (ie.
 *              {@link org.elasticsearch.xpack.core.ilm.ClusterStateActionStep} and
 *              {@link org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep}). This is triggered by the
 *              {@link org.elasticsearch.xpack.ilm.IndexLifecycleService#clusterChanged(org.elasticsearch.cluster.ClusterChangedEvent)}
 *              callback.
 *          </li>
 *          <li>
 *              Run the {@link org.elasticsearch.xpack.core.ilm.AsyncWaitStep} periodic steps. These steps are configured to run
 *              every {@link org.elasticsearch.xpack.core.ilm.LifecycleSettings#LIFECYCLE_POLL_INTERVAL}
 *          </li>
 *      </ul>
 *  </li>
 * </ul>
 *
 * The {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner} is the component that executes the ILM steps. It has 3 entry points that
 * correspond to the steps taxonomy outlined above. Namely:
 * <ul>
 *     <li>
 *          {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner#maybeRunAsyncAction(
 *                      org.elasticsearch.cluster.ClusterState,
 *                      org.elasticsearch.cluster.metadata.IndexMetaData,
 *                      java.lang.String, org.elasticsearch.xpack.core.ilm.Step.StepKey
 *                  )}
 *          handles the execution of the async steps {@link org.elasticsearch.xpack.core.ilm.AsyncActionStep}.
 *     </li>
 *     <li>
 *         {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner#runPolicyAfterStateChange(
 *                      java.lang.String,
 *                      org.elasticsearch.cluster.metadata.IndexMetaData
 *                  )}
 *         handles the execution of steps that wait or need to react to cluster state changes, like
 *         {@link org.elasticsearch.xpack.core.ilm.ClusterStateActionStep} and {@link org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep}
 *     </li>
 *     <li>
 *        {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner#runPeriodicStep(
 *                      java.lang.String,
 *                      org.elasticsearch.cluster.metadata.IndexMetaData
 *                 )}
 *        handles the execution of async {@link org.elasticsearch.xpack.core.ilm.AsyncWaitStep}
 *     </li>
 * </ul>
 *
 * The policy execution can be seen as a state machine which advances through every phase's (hot/warm/cold/delete) action's
 * (rollover/forcemerge/etc) steps (eg. the {@link org.elasticsearch.xpack.core.ilm.RolloverAction} comprises a series of steps that need
 * to be executed. It will first check if the rollover could be executed {@link org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep}
 * and then rollover the index {@link org.elasticsearch.xpack.core.ilm.RolloverStep} followed by some more house-keeping steps).
 *
 * The ILM runner will advance last executed state (as indicated in
 * {@link org.elasticsearch.xpack.core.ilm.LifecycleExecutionState#getStep()}) and execute the next step of the index policy as
 * defined in the {@link org.elasticsearch.xpack.ilm.PolicyStepsRegistry}.
 * Once all the steps of a policy are executed successfully the policy execution will reach the
 * {@link org.elasticsearch.xpack.core.ilm.TerminalPolicyStep} and any changes made to the policy definition will not have any effect on
 * the already completed policies. Even more, any changes made to the policy HOT phase will have *no* effect on the already in-progress HOT
 * phase executions (the phase JSON representation being cached into the index metadata). However, a policy update to the WARM phase will
 * *have* an effect on the policies that are currently in the HOT execution state as the entire WARM phase will be reloaded from the
 * policy definition when transitioning to the phase.
 *
 * If a step execution fails, the policy execution state for the index will be moved into the
 * {@link org.elasticsearch.xpack.core.ilm.ErrorStep}.
 * Currently for certain periodic steps we will automatically retry the execution of the failed step until the step executes
 * successfully (see {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner#onErrorMaybeRetryFailedStep}). In order to see all retryable
 * steps see {@link org.elasticsearch.xpack.core.ilm.Step#isRetryable()}.
 * For steps that are not retryable the failed step can manually be retried using
 * {@link org.elasticsearch.xpack.ilm.IndexLifecycleService#moveClusterStateToPreviouslyFailedStep}.
 *
 */
package org.elasticsearch.xpack.ilm;

