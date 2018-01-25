/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import java.util.function.LongSupplier;

public abstract class MockIndexLifecycleContext implements IndexLifecycleContext {

    private final String targetName;
    private String phase;
    private String action;
    private Exception exceptionToThrow;
    private int numberOfReplicas;
    private LongSupplier nowSupplier;
    private long phaseTime;
    private long actionTime;

    public MockIndexLifecycleContext(String targetName, String initialPhase, String initialAction, int numberOfReplicas,
                                     LongSupplier nowSupplier) {
        this.targetName = targetName;
        this.phase = initialPhase;
        this.action = initialAction;
        this.numberOfReplicas = numberOfReplicas;
        this.nowSupplier = nowSupplier;
        this.phaseTime = -1L;
        this.actionTime = -1L;
    }

    public void failOnSetters(Exception exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }

    @Override
    public void setPhase(String phase, Listener listener) {
        if (exceptionToThrow != null) {
            listener.onFailure(exceptionToThrow);
            return;
        }
        this.phase = phase;
        this.phaseTime = nowSupplier.getAsLong();
        this.action = "";
        this.actionTime = -1L;
        listener.onSuccess();
    }

    @Override
    public void setAction(String action, Listener listener) {
        if (exceptionToThrow != null) {
            listener.onFailure(exceptionToThrow);
            return;
        }
        this.action = action;
        this.actionTime = nowSupplier.getAsLong();
        listener.onSuccess();
    }

    @Override
    public String getAction() {
        return action;
    }

    @Override
    public long getActionTime() {
        return actionTime;
    }

    @Override
    public String getPhase() {
        return phase;
    }

    @Override
    public long getPhaseTime() {
        return phaseTime;
    }

    @Override
    public String getLifecycleTarget() {
        return targetName;
    }

    @Override
    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public abstract boolean canExecute(Phase phase);

    @Override
    public void executeAction(LifecycleAction action, LifecycleAction.Listener listener) {
        action.execute(null, null, null, listener);
    }

}
