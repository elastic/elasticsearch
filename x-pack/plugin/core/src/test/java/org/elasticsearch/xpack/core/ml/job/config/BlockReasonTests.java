/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class BlockReasonTests extends ESTestCase {

    public void testFromString() {
        assertThat(BlockReason.fromString("dElETe"), equalTo(BlockReason.DELETE));
        assertThat(BlockReason.fromString("ReSEt"), equalTo(BlockReason.RESET));
        assertThat(BlockReason.fromString("reVERt"), equalTo(BlockReason.REVERT));
    }

    public void testToString() {
        List<String> asStrings = Arrays.stream(BlockReason.values()).map(BlockReason::toString).collect(Collectors.toList());
        assertThat(asStrings, contains("delete", "reset", "revert"));
    }

}
