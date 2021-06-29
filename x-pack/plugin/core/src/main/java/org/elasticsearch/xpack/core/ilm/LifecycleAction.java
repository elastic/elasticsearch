/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.license.XPackLicenseState;

import java.util.List;

/**
 * Executes an action on an index related to its lifecycle.
 */
public interface LifecycleAction extends ToXContentObject, NamedWriteable {

    /**
     * converts the {@link LifecycleAction}'s execution plan into a series of
     * {@link Step}s that reference each other to preserve order of operations.
     * @param client      the client that will be used by {@link AsyncActionStep} and {@link AsyncWaitStep} steps
     * @param phase       the name of the phase this action is being executed within
     * @param nextStepKey the next step to execute after this action's steps. If null, then there are no further
     *                    steps to run. It is the responsibility of each {@link LifecycleAction} to implement this
     *                    correctly and not forget to link to this final step so that the policy can continue.
     * @return an ordered list of steps that represent the execution plan of the action
     */
    List<Step> toSteps(Client client, String phase, @Nullable Step.StepKey nextStepKey);

    /**
     * converts the {@link LifecycleAction}'s execution plan into a series of
     * {@link Step}s that reference each other to preserve order of operations. This overload allows access to license state.
     * @param client      the client that will be used by {@link AsyncActionStep} and {@link AsyncWaitStep} steps
     * @param phase       the name of the phase this action is being executed within
     * @param nextStepKey the next step to execute after this action's steps. If null, then there are no further
     *                    steps to run. It is the responsibility of each {@link LifecycleAction} to implement this
     *                    correctly and not forget to link to this final step so that the policy can continue.
     * @param licenseState the license state to use for actions that need license checks.
     * @return an ordered list of steps that represent the execution plan of the action
     */
    default List<Step> toSteps(Client client, String phase, @Nullable Step.StepKey nextStepKey, XPackLicenseState licenseState) {
        return toSteps(client, phase, nextStepKey);
    }

    /**
     * @return true if this action is considered safe. An action is not safe if
     *         it will produce unwanted side effects or will get stuck when the
     *         action configuration is changed while an index is in this action
     */
    boolean isSafeAction();
}
