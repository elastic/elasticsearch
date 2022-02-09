/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class AssociatedIndexDescriptorTests extends ESTestCase {

    /**
     * Tests the various validation rules that are applied when creating a new associated index descriptor.
     */
    public void testValidation() {
        {
            Exception ex = expectThrows(NullPointerException.class, () -> new AssociatedIndexDescriptor(null, randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new AssociatedIndexDescriptor("", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new AssociatedIndexDescriptor(".", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new AssociatedIndexDescriptor(randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new AssociatedIndexDescriptor(".*", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new AssociatedIndexDescriptor(".*" + randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
    }

    public void testSpecialCharactersAreReplacedWhenConvertingToAutomaton() {
        CharacterRunAutomaton automaton = new CharacterRunAutomaton(AssociatedIndexDescriptor.buildAutomaton(".associated-index*"));

        // None of these should match, ever.
        assertFalse(automaton.run(".my-associated-index"));
        assertFalse(automaton.run("my.associated-index"));
        assertFalse(automaton.run("some-other-index"));

        // These should only fail if the trailing `*` doesn't get properly replaced with `.*`
        assertTrue("if the trailing * isn't replaced, suffixes won't match properly", automaton.run(".associated-index-1"));
        assertTrue("if the trailing * isn't replaced, suffixes won't match properly", automaton.run(".associated-index-asdf"));

        // These should only fail if the leading `.` doesn't get properly replaced with `\\.`
        assertFalse("if the leading dot isn't replaced, it can match date math", automaton.run("<associated-index-{now/d}>"));
        assertFalse("if the leading dot isn't replaced, it can match any single-char prefix", automaton.run("Oassociated-index"));
        assertFalse("the leading dot got dropped", automaton.run("associated-index-1"));
    }
}
