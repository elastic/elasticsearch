/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ValidatingSubstitutorTests extends ESTestCase {
    public void testReplace() {
        var sub = new ValidatingSubstitutor(Map.of("key", "value", "key2", "value2"), "${", "}");
        assertThat(sub.replace("super:${key}", "setting"), is("super:value"));
        assertThat(sub.replace("super, ${key}, ${key2}", "setting"), is("super, value, value2"));
        assertThat(sub.replace("super", "setting"), is("super"));
    }

    public void testReplace_MatchesComplexPlaceholder() {
        var sub = new ValidatingSubstitutor(Map.of("\t\b\f'\"\\key", "value"), "${", "}");
        assertThat(sub.replace("super, ${\t\b\f'\"\\key}", "setting"), is("super, value"));
    }

    public void testReplace_IgnoresPlaceholdersWithNewlines() {
        var sub = new ValidatingSubstitutor(Map.of("key", "value", "key2", "value2"), "${", "}");
        assertThat(sub.replace("super:${key\n}", "setting"), is("super:${key\n}"));
        assertThat(sub.replace("super:${\nkey}", "setting"), is("super:${\nkey}"));
    }

    public void testReplace_ThrowsException_WhenPlaceHolderStillExists() {
        {
            var sub = new ValidatingSubstitutor(Map.of("some_key", "value", "key2", "value2"), "${", "}");
            var exception = expectThrows(IllegalStateException.class, () -> sub.replace("super:${key}", "setting"));

            assertThat(exception.getMessage(), is("Found placeholder [${key}] in field [setting] after replacement call"));
        }
        // only reports the first placeholder pattern
        {
            var sub = new ValidatingSubstitutor(Map.of("some_key", "value", "some_key2", "value2"), "${", "}");
            var exception = expectThrows(IllegalStateException.class, () -> sub.replace("super, ${key}, ${key2}", "setting"));

            assertThat(exception.getMessage(), is("Found placeholder [${key}] in field [setting] after replacement call"));
        }
        {
            var sub = new ValidatingSubstitutor(Map.of("some_key", "value", "key2", "value2"), "${", "}");
            var exception = expectThrows(IllegalStateException.class, () -> sub.replace("super:${     \\/\tkey\"}", "setting"));

            assertThat(exception.getMessage(), is("Found placeholder [${     \\/\tkey\"}] in field [setting] after replacement call"));
        }
    }
}
