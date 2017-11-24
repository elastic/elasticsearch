/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

public abstract class MockIndexLifecycleContext implements IndexLifecycleContext {

    private final String targetName;
    private String phase;
    private String action;
    private Exception exceptionToThrow;

    public MockIndexLifecycleContext(String targetName, String initialPhase, String initialAction) {
        this.targetName = targetName;
        this.phase = initialPhase;
        this.action = initialAction;
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
        this.action = "";
        listener.onSuccess();
    }

    @Override
    public void setAction(String action, Listener listener) {
        if (exceptionToThrow != null) {
            listener.onFailure(exceptionToThrow);
            return;
        }
        this.action = action;
        listener.onSuccess();
    }

    @Override
    public String getAction() {
        return action;
    }

    @Override
    public String getPhase() {
        return phase;
    }

    @Override
    public String getLifecycleTarget() {
        return targetName;
    }

    @Override
    public void executeAction(LifecycleAction action) {
        action.execute(null, null);
    }

}
