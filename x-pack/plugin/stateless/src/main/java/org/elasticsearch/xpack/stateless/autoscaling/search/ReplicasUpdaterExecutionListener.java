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

package org.elasticsearch.xpack.stateless.autoscaling.search;

/**
* Hook interface for testing and observability of the replicas updater service.
* Production code uses {@link NoopExecutionListener}.
* Tests can inject custom implementations to control execution timing.
*/
interface ReplicasUpdaterExecutionListener {
    /**
     * Called when the replicas updater loop starts executing
     */
    void onRunStart(boolean immediateScaleDown, boolean onlyScaleDownToTopologyBounds);

    /**
     * Called when the loop completes
     */
    void onRunComplete();

    /**
     * Called trigger to run the loop is skipped because already running
     */
    void onSkipped();

    /**
     * Called when the currently running loop executes a pending request to run
     */
    void onPendingRunExecution();

    /**
     * Called immediately after the state transitions to IDLE
     */
    void onStateTransitionToIdle();

    class NoopExecutionListener implements ReplicasUpdaterExecutionListener {

        public static final NoopExecutionListener INSTANCE = new NoopExecutionListener();

        @Override
        public void onRunStart(boolean immediateScaleDown, boolean onlyScaleDownToTopologyBounds) {}

        @Override
        public void onRunComplete() {}

        @Override
        public void onSkipped() {}

        @Override
        public void onPendingRunExecution() {}

        @Override
        public void onStateTransitionToIdle() {}
    }
}
