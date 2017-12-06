/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

/**
 * Provides the context to a {@link LifecyclePolicy} for a particular target.
 * This context provides the state of the lifecycle target (hereafter referred
 * to as the target) as well as allow operations to be performed on the target.
 */
public interface IndexLifecycleContext {

    /**
     * Sets the phase for the target and calls the provided callback. Note that
     * this will also set the action name to an empty {@link String}.
     * 
     * @param phase
     *            the name of the phase to be set.
     * @param listener
     *            a {@link Listener} to call after the operation.
     */
    void setPhase(String phase, Listener listener);

    /**
     * Sets the action for the target and calls the provided callback.
     * 
     * @param action
     *            the name of the action to be set.
     * @param listener
     *            a {@link Listener} to call after the operation.
     */
    void setAction(String action, Listener listener);

    /**
     * @return the current {@link LifecycleAction} name for the target.
     */
    String getAction();

    /**
     * @return the current {@link Phase} name for the target.
     */
    String getPhase();

    /**
     * @return the name of the target.
     */
    String getLifecycleTarget();

    /**
     * Determines whether the target is able to move to the provided
     * {@link Phase}
     * 
     * @param phase
     *            the {@link Phase} to test
     * @return <code>true</code> iff the target is ready to move to the
     *         {@link Phase}.
     */
    boolean canExecute(Phase phase);

    /**
     * Executes the provided {@link LifecycleAction} passing the relevant target
     * state to it.
     * 
     * @param action
     *            the {@link LifecycleAction} to execute.
     * @param listener
     *            a {@link LifecycleAction.Listener} to pass to the
     *            {@link LifecycleAction}.
     */
    void executeAction(LifecycleAction action, LifecycleAction.Listener listener);

    /**
     * A callback for use when setting phase or action names.
     */
    interface Listener {

        /**
         * Called if the call to set the action/phase name was successful.
         */
        void onSuccess();

        /**
         * Called if there was an exception when setting the action or phase
         * name.
         * 
         * @param e
         *            the exception that caused the failure
         */
        void onFailure(Exception e);
    }
}
