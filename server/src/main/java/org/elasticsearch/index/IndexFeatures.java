/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Defines the set of features supported by Elasticsearch indices.
 * This class provides both production and test features for index functionality.
 */
public class IndexFeatures implements FeatureSpecification {

    /**
     * Retrieves the set of production node features supported by indices.
     *
     * @return an empty set as all index features are currently test-only
     */
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    /**
     * Feature flag indicating that logsdb mode does not automatically add a host.name field.
     */
    public static final NodeFeature LOGSDB_NO_HOST_NAME_FIELD = new NodeFeature("index.logsdb_no_host_name_field");

    /**
     * Feature flag for setting synonyms to be lenient on non-existing terms.
     */
    private static final NodeFeature SYNONYMS_SET_LENIENT_ON_NON_EXISTING = new NodeFeature("index.synonyms_set_lenient_on_non_existing");

    /**
     * Feature flag for throwing exceptions when unknown tokens are encountered in REST index put alias actions.
     */
    private static final NodeFeature THROW_EXCEPTION_FOR_UNKNOWN_TOKEN_IN_REST_INDEX_PUT_ALIAS_ACTION = new NodeFeature(
        "index.throw_exception_for_unknown_token_in_rest_index_put_alias_action"
    );

    /**
     * Feature flag for throwing exceptions on index creation if alias contains unsupported value types.
     */
    private static final NodeFeature THROW_EXCEPTION_ON_INDEX_CREATION_IF_UNSUPPORTED_VALUE_TYPE_IN_ALIAS = new NodeFeature(
        "index.throw_exception_on_index_creation_if_unsupported_value_type_in_alias"
    );

    /**
     * Retrieves the set of test-only node features supported by indices.
     * These features are used for testing and validation purposes.
     *
     * @return a set containing all test features for index functionality
     */
    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(
            LOGSDB_NO_HOST_NAME_FIELD,
            SYNONYMS_SET_LENIENT_ON_NON_EXISTING,
            THROW_EXCEPTION_FOR_UNKNOWN_TOKEN_IN_REST_INDEX_PUT_ALIAS_ACTION,
            THROW_EXCEPTION_ON_INDEX_CREATION_IF_UNSUPPORTED_VALUE_TYPE_IN_ALIAS
        );
    }
}
