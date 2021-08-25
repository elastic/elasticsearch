/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class ImmutableOpenMapTests extends ESTestCase {

    ImmutableOpenMap<String, String> regionCurrencySymbols = ImmutableOpenMap.<String, String>builder()
        .fPut("Japan", "¥")
        .fPut("USA", "$")
        .fPut("EU", "€")
        .fPut("UK", "£")
        .fPut("Korea", "₩")
        .build();

    public void testStreamOperationsAreSupported() {
        assertThat(regionCurrencySymbols.stream().filter(e -> e.key.startsWith("U")).map(e -> e.value).collect(Collectors.toSet()),
            equalTo(Set.of("£", "$")));
    }

    public void testEmptyStreamWorks() {
        assertThat(ImmutableOpenMap.of().stream().count(), equalTo(0L));
    }
}
