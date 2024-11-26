/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import java.util.List;

/**
 * Allows to register additional features supported by the tests runner.
 * This way any runner can add extra features and use proper skip sections to avoid
 * breaking others runners till they have implemented the new feature as well.
 *
 * Once all runners have implemented the feature, it can be removed from the list
 * and the related skip sections can be removed from the tests as well.
 */
public final class Features {

    private static final List<String> SUPPORTED = List.of(
        "catch_unauthorized",
        "default_shards",
        "embedded_stash_key",
        "headers",
        "node_selector",
        "stash_in_key",
        "stash_in_path",
        "stash_path_replace",
        "warnings",
        "warnings_regex",
        "yaml",
        "contains",
        "transform_and_set",
        "arbitrary_key",
        "allowed_warnings",
        "allowed_warnings_regex",
        "close_to",
        "is_after",
        "capabilities"
    );

    private Features() {

    }

    /**
     * Tells whether all the features provided as argument are supported
     */
    public static boolean areAllSupported(List<String> features) {
        for (String feature : features) {
            if (false == SUPPORTED.contains(feature)) {
                return false;
            }
        }
        return true;
    }
}
