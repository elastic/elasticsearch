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

public class IndexFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    public static final NodeFeature LOGSDB_NO_HOST_NAME_FIELD = new NodeFeature("index.logsdb_no_host_name_field");

    private static final NodeFeature SYNONYMS_SET_LENIENT_ON_NON_EXISTING = new NodeFeature("index.synonyms_set_lenient_on_non_existing");

    private static final NodeFeature THROW_EXCEPTION_FOR_UNKNOWN_TOKEN_IN_REST_INDEX_PUT_ALIAS_ACTION = new NodeFeature(
        "index.throw_exception_for_unknown_token_in_rest_index_put_alias_action"
    );

    private static final NodeFeature THROW_EXCEPTION_ON_INDEX_CREATION_IF_UNSUPPORTED_VALUE_TYPE_IN_ALIAS = new NodeFeature(
        "index.throw_exception_on_index_creation_if_unsupported_value_type_in_alias"
    );

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
