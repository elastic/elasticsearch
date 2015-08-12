/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class MarvelVersionTests extends ESTestCase {

    @Test
    public void testVersionFromString() {
        assertThat(MarvelVersion.fromString("2.0.0"), equalTo(MarvelVersion.V_2_0_0));
    }

    @Test
    public void testVersionNumber() {
        assertThat(MarvelVersion.V_2_0_0.number(), equalTo("2.0.0"));
    }
}