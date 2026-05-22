/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class SynonymFeatures implements FeatureSpecification {
    private static final NodeFeature RETURN_EMPTY_SYNONYM_SETS = new NodeFeature("synonyms_set.get.return_empty_synonym_sets");
    private static final NodeFeature MAX_SYNONYM_RULES_SETTING = new NodeFeature("synonyms.max_synonym_rules");
    private static final NodeFeature SYNONYMS_GET_SEARCH_AFTER = new NodeFeature("synonyms.get.search_after");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RETURN_EMPTY_SYNONYM_SETS, MAX_SYNONYM_RULES_SETTING, SYNONYMS_GET_SEARCH_AFTER);
    }
}
