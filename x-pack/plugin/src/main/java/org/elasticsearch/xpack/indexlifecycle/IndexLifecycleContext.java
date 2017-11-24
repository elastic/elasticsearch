/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

public interface IndexLifecycleContext {

    void setPhase(String phase, Listener listener);

    void setAction(String action, Listener listener);

    String getAction();

    String getPhase();

    String getLifecycleTarget();

    boolean canExecute(Phase phase);

    public void executeAction(LifecycleAction action);

    public static interface Listener {

        void onSuccess();

        void onFailure(Exception e);
    }
}
