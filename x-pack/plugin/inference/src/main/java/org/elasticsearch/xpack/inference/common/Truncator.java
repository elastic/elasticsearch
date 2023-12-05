/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import java.util.List;

/**
 * Provides truncation logic for inference requests
 */
public class Truncator {

    public static List<String> truncate(List<String> input, double reductionPercentage) {
        assert reductionPercentage < 1 && reductionPercentage > 0 : "reduction percentage must be > 0 && < 1";

        return input.stream().map(text -> {
            var length = (int) Math.floor(text.length() * reductionPercentage);
            return text.substring(0, Math.min(text.length(), length));
        }).toList();
    }

    private Truncator() {}
}
