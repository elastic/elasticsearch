/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import joptsimple.OptionSet;

import org.apache.lucene.tests.util.TestRuleRestoreSystemProperties;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class EvilEnvironmentAwareCommandTests extends ESTestCase {

    @Rule
    public TestRule restoreSystemProperties = new TestRuleRestoreSystemProperties("es.path.conf");

    public void testEsPathConfNotSet() throws Exception {
        clearEsPathConf();

        class TestEnvironmentAwareCommand extends EnvironmentAwareCommand {

            private TestEnvironmentAwareCommand(String description) {
                super(description);
            }

            @Override
            protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

            }

        }

        final TestEnvironmentAwareCommand command = new TestEnvironmentAwareCommand("test");
        final UserException e = expectThrows(
            UserException.class,
            () -> command.mainWithoutErrorHandling(new String[0], new MockTerminal())
        );
        assertThat(e, hasToString(containsString("the system property [es.path.conf] must be set")));
    }

    @SuppressForbidden(reason = "clears system property es.path.conf as part of test setup")
    private void clearEsPathConf() {
        System.clearProperty("es.path.conf");
    }

}
