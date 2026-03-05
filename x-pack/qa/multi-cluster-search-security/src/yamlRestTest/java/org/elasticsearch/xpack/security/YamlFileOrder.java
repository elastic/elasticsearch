/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;

import java.util.Comparator;

/**
 * A comparator to order YAML tests alphabetically by their file names.
 */
public class YamlFileOrder implements Comparator<TestMethodAndParams> {
    @Override
    public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
        return o1.getInstanceArguments().isEmpty() || o2.getInstanceArguments().isEmpty()
            ? 0
            : o1.getInstanceArguments().getFirst().toString().compareTo(o2.getInstanceArguments().getFirst().toString());
    }
}
