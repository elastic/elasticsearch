/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.rest.ESRestTestCase;

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
            "yaml",
            "contains",
            "transform_and_set",
            "arbitrary_key");

    private Features() {

    }

    /**
     * Tells whether all the features provided as argument are supported
     */
    public static boolean areAllSupported(List<String> features) {
        for (String feature : features) {
            if (feature.equals("xpack")) {
                if (false == ESRestTestCase.hasXPack()) {
                    return false;
                }
            } else if (feature.equals("no_xpack")) {
                if (ESRestTestCase.hasXPack()) {
                    return false;
                }
            } else if (false == SUPPORTED.contains(feature)) {
                return false;
            }
        }
        return true;
    }
}
