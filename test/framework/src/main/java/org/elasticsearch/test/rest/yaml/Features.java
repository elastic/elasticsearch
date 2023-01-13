/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Allows to register additional features supported by the tests runner.
 * This way any runner can add extra features and use proper skip sections to avoid
 * breaking others runners till they have implemented the new feature as well.
 *
 * Once all runners have implemented the feature, it can be removed from the list
 * and the related skip sections can be removed from the tests as well.
 */
public final class Features {
    private static final List<String> SUPPORTED = unmodifiableList(
        Arrays.asList(
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
            "close_to"
        )
    );
    private static final String SPI_ON_CLASSPATH_SINCE_JDK_9 = "spi_on_classpath_jdk9";

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
            } else if (false == isSupported(feature)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSupported(String feature) {
        String runtimeJavaProp = System.getProperty("tests.runtime.java.version");
        JavaVersion runtimeJava = runtimeJavaProp != null ? JavaVersion.parse(runtimeJavaProp) : JavaVersion.current();
        if (feature.equals(SPI_ON_CLASSPATH_SINCE_JDK_9) && runtimeJava.compareTo(JavaVersion.parse("9")) >= 0) {
            return true;
        }
        return SUPPORTED.contains(feature);
    }
}
