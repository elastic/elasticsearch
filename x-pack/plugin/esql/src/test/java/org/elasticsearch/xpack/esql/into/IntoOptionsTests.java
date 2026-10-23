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

    public void testNamesAndCollisionsAreCaseInsensitive() {
        IntoOptions options = IntoOptions.DEFAULT.with("Collisions", "KEEP").with("REFRESH", false).with("Keep_Going", true);
        assertThat(options, equalTo(new IntoOptions(IntoOptions.Collisions.KEEP, false, true, false)));
        assertTrue(options.with("APPROXIMATE", true).approximate());
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

    /** Values are typed: a boolean option takes a boolean, not the text of one, and collisions takes a string. */
    public void testBadValues() {
        var e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("collisions", "merge"));
        assertThat(e.getMessage(), containsString("invalid value [merge] for option [collisions]"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("collisions", true));
        assertThat(e.getMessage(), containsString("invalid value [true] for option [collisions]"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("refresh", "true"));
        assertThat(e.getMessage(), containsString("invalid value [true] for option [refresh], expected a boolean"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("keep_going", 1));
        assertThat(e.getMessage(), containsString("invalid value [1] for option [keep_going]"));
        e = expectThrows(IllegalArgumentException.class, () -> IntoOptions.DEFAULT.with("approximate", "0"));
        assertThat(e.getMessage(), containsString("invalid value [0] for option [approximate]"));
    }
}
