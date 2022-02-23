/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.readiness;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.readiness.ReadinessService;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // Don't randomly add 'extra' files to directory.
public class ReadinessProbeCliTests extends LuceneTestCase {

    @BeforeClass
    @SuppressForbidden(reason = "need to set sys props for CLI tool")
    public static void setSystemPropsForTool() {
        Path configPath = Paths.get("tmp", "config");
        System.setProperty("es.path.conf", configPath.toString());
        System.setProperty("es.path.home", configPath.getParent().toString());
    }

    public void testNoArguments() throws Exception {
        MockTerminal terminal = new MockTerminal();
        ReadinessProbeCli cli = new ReadinessProbeCli();
        cli = spy(cli);
        doAnswer(i -> {
            Object arg0 = i.getArgument(0);

            assertThat(arg0.toString(), containsString(ReadinessService.SOCKET_NAME));
            return null;
        }).when(cli).tryConnect(any());

        cli.main(new String[] {}, terminal);
        assertThat(terminal.getErrorOutput(), emptyString());
    }

}
