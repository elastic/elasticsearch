/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy.NextActionProvider;

import java.util.Collection;
import java.util.Map;

public interface LifecycleType extends NamedWriteable {

    /**
     * @return the first phase of this policy to execute
     */
    Phase getFirstPhase(Map<String, Phase> phases);

    /**
     * @param currentPhase
     *            the current phase that is or was just executed
     * @return the next phase after <code>currentPhase</code> to be execute. If
     *         it is `null`, the first phase to be executed is returned. If it
     *         is the last phase, then no next phase is to be executed and
     *         `null` is returned.
     */
    Phase nextPhase(Map<String, Phase> phases, @Nullable Phase currentPhase);

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

    /**
     * Each {@link LifecyclePolicy} has a specific type to differentiate
     * themselves. Every implementation is responsible to providing its specific
     * type.
     * 
     * @return the {@link LifecyclePolicy} type.
     */
    String getType();

    /**
     * @param context
     *            the index lifecycle context for this phase at the time of
     *            execution
     * @param phase
     *            the current phase for which to provide an action provider
     * @return the action provider
     */
    NextActionProvider getActionProvider(IndexLifecycleContext context, Phase phase);
}
