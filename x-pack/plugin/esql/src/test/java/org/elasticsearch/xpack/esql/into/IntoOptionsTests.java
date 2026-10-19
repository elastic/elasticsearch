/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntoOptionsTests extends ESTestCase {

    public void testDefaults() {
        assertThat(IntoOptions.DEFAULT.collisions(), equalTo(IntoOptions.Collisions.ABORT));
        assertTrue(IntoOptions.DEFAULT.refresh());
        assertFalse(IntoOptions.DEFAULT.keepGoing());
        assertFalse(IntoOptions.DEFAULT.approximate());
    }

    public void testNamesAndValuesAreCaseInsensitive() {
        IntoOptions options = IntoOptions.DEFAULT.with("Collisions", "KEEP").with("REFRESH", "False").with("Keep_Going", "TRUE");
        assertThat(options, equalTo(new IntoOptions(IntoOptions.Collisions.KEEP, false, true, false)));
        assertTrue(options.with("APPROXIMATE", "true").approximate());
        assertThat(IntoOptions.DEFAULT.with("collisions", "Replace").collisions(), equalTo(IntoOptions.Collisions.REPLACE));
        assertThat(IntoOptions.DEFAULT.with("collisions", "abort").collisions(), equalTo(IntoOptions.Collisions.ABORT));
    }

    public void testUnknownOption() {
        var e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("mode", "keep"));
        assertThat(
            e.getMessage(),
            equalTo("unknown option [mode] for INTO, expected one of [collisions, refresh, keep_going, approximate]")
        );
    }

    public void testBadValues() {
        var e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("collisions", "merge"));
        assertThat(e.getMessage(), containsString("invalid value [merge] for option [collisions]"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("refresh", "yes"));
        assertThat(e.getMessage(), containsString("invalid value [yes] for option [refresh]"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("keep_going", "maybe"));
        assertThat(e.getMessage(), containsString("invalid value [maybe] for option [keep_going]"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("approximate", "0"));
        assertThat(e.getMessage(), containsString("invalid value [0] for option [approximate]"));
    }
}
