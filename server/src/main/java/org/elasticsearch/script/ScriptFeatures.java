/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public final class ScriptFeatures implements FeatureSpecification {

    /**
     * Scripted updates accept documents whose source has top-level fields with the same names as update metadata keys,
     * such as {@code _type}, {@code op} or {@code _now}.
     */
    public static final NodeFeature SOURCE_FIELDS_NAMED_LIKE_METADATA = new NodeFeature("script.source_fields_named_like_metadata");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(SOURCE_FIELDS_NAMED_LIKE_METADATA);
    }
}
