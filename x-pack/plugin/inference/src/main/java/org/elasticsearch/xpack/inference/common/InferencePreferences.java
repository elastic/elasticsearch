/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;

/**
 * User-configurable preferences that apply across inference requests for a single project, such as the
 * region policy used to set region-restriction headers on outgoing Elastic Inference Service requests.
 * Cached per-project by {@link InferencePreferencesCache}. Has no wire or storage format of its own, so
 * new {@code @Nullable} fields can be added here as more preference types are introduced.
 */
public record InferencePreferences(@Nullable RegionPolicy regionPolicy) {

    public static final InferencePreferences EMPTY = new InferencePreferences(null);
}
