/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class MarvelVersionTests extends ElasticsearchTestCase {

    @Test
    public void testVersionFromString() {
        assertThat(MarvelVersion.fromString("2.0.0-beta1"), equalTo(MarvelVersion.V_2_0_0_Beta1));
    }

    @Test
    public void testVersionNumber() {
        assertThat(MarvelVersion.V_2_0_0_Beta1.number(), equalTo("2.0.0-beta1"));
    }
}