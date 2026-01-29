/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;

import java.util.function.Predicate;

/**
 * SPI service interface for providing template settings filters to optional ignore some settings in Stateless mode.
 */
public interface MetadataIndexTemplateSettingsFilterProvider {

    static MetadataIndexTemplateSettingsFilterProvider defaultProvider() {
        return Predicates::always;
    }

    /**
     * @return The settings key predicate
     */
    @Nullable
    Predicate<String> getFilter();
}
