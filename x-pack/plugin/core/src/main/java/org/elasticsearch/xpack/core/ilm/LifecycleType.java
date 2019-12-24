/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.NamedWriteable;

import java.security.Policy;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface LifecycleType extends NamedWriteable {

    /**
     * @return the first phase of this policy to execute
     */
    List<Phase> getOrderedPhases(Map<String, Phase> phases);

    /**
     * Returns the next phase thats available after
     * <code>currentPhaseName</code>. Note that <code>currentPhaseName</code>
     * does not need to exist in <code>phases</code>.
     *
     * If the current {@link Phase} is the last phase in the {@link Policy} this
     * method will return <code>null</code>.
     *
     * If the phase is not valid for the lifecycle type an
     * {@link IllegalArgumentException} will be thrown.
     */
    String getNextPhaseName(String currentPhaseName, Map<String, Phase> phases);

    /**
     * Returns the previous phase thats available before
     * <code>currentPhaseName</code>. Note that <code>currentPhaseName</code>
     * does not need to exist in <code>phases</code>.
     *
     * If the current {@link Phase} is the first phase in the {@link Policy}
     * this method will return <code>null</code>.
     *
     * If the phase is not valid for the lifecycle type an
     * {@link IllegalArgumentException} will be thrown.
     */
    String getPreviousPhaseName(String currentPhaseName, Map<String, Phase> phases);

    List<LifecycleAction> getOrderedActions(Phase phase);

    /**
     * Returns the name of the next phase that is available in the phases after
     * <code>currentActionName</code>. Note that <code>currentActionName</code>
     * does not need to exist in the {@link Phase}.
     *
     * If the current action is the last action in the phase this method will
     * return <code>null</code>.
     *
     * If the action is not valid for the phase an
     * {@link IllegalArgumentException} will be thrown.
     */
    String getNextActionName(String currentActionName, Phase phase);


    /**
     * validates whether the specified <code>phases</code> are valid for this
     * policy instance.
     *
     * @param phases
     *            the phases to verify validity against
     * @throws IllegalArgumentException
     *             if a specific phase or lack of a specific phase is invalid.
     */
    void validate(Collection<Phase> phases);
}
