/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.hamcrest;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.StringDescription;

import java.util.Optional;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class OptionalMatchersTests extends ESTestCase {

    public void testEmptyMatcher() {
        assertThat(Optional.empty(), isEmpty());
        assertThat(Optional.of(""), not(isEmpty()));

        StringDescription desc = new StringDescription();
        isEmpty().describeMismatch(Optional.of(""), desc);
        assertThat(desc.toString(), equalTo("a non-empty optional \"\""));
    }

    public void testIsPresentMatcher() {
        assertThat(Optional.of(""), isPresent());
        assertThat(Optional.empty(), not(isPresent()));

        StringDescription desc = new StringDescription();
        isPresent().describeMismatch(Optional.empty(), desc);
        assertThat(desc.toString(), equalTo("an empty optional"));
    }

    public void testIsPresentWithMatcher() {
        assertThat(Optional.of(""), isPresentWith(""));
        assertThat(Optional.of("foo"), not(isPresentWith("")));
        assertThat(Optional.empty(), not(isPresentWith("")));

        StringDescription desc = new StringDescription();
        isPresentWith("foo").describeMismatch(Optional.empty(), desc);
        assertThat(desc.toString(), equalTo("an empty optional"));

        desc = new StringDescription();
        isPresentWith("foo").describeMismatch(Optional.of(""), desc);
        assertThat(desc.toString(), equalTo("an optional was \"\""));
    }
}
