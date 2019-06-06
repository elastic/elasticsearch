/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

import joptsimple.OptionSet;
import org.apache.lucene.util.TestRuleRestoreSystemProperties;
import org.elasticsearch.common.SuppressForbidden;
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
        final UserException e =
                expectThrows(UserException.class, () -> command.mainWithoutErrorHandling(new String[0], new MockTerminal()));
        assertThat(e, hasToString(containsString("the system property [es.path.conf] must be set")));
    }

    @SuppressForbidden(reason =  "clears system property es.path.conf as part of test setup")
    private void clearEsPathConf() {
        System.clearProperty("es.path.conf");
    }

}
