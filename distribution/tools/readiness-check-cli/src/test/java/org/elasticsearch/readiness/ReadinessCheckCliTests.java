/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.MockTerminal;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // Don't randomly add 'extra' files to directory.
public class ReadinessCheckCliTests extends LuceneTestCase {

    public void testNoArguments() throws Exception {
        MockTerminal terminal = new MockTerminal();
        ReadinessCheckCli cli = new ReadinessCheckCli();
        cli = spy(cli);
        doAnswer(i -> {
            Object arg0 = i.getArgument(0);
            Object arg1 = i.getArgument(1);

            assertEquals("localhost", arg0);
            assertEquals(9200, arg1);
            return null;
        }).when(cli).tryConnect(any(), any());

        cli.main(new String[] {}, terminal);
        assertThat(terminal.getErrorOutput(), emptyString());
    }

    public void testFailOnEmptyArg() throws Exception {
        MockTerminal terminal = new MockTerminal();
        ReadinessCheckCli cli = new ReadinessCheckCli();
        cli.main(new String[] { "-h" }, terminal);
        assertThat(terminal.getErrorOutput(), containsString("ERROR: Option h/host requires an argument"));
        cli.main(new String[] { "--port" }, terminal);
        assertThat(terminal.getErrorOutput(), containsString("ERROR: Option p/port requires an argument"));
    }

    public void testCustomArguments() throws Exception {
        MockTerminal terminal = new MockTerminal();
        ReadinessCheckCli cli = new ReadinessCheckCli();
        cli = spy(cli);
        doAnswer(i -> {
            Object arg0 = i.getArgument(0);
            Object arg1 = i.getArgument(1);

            assertEquals("192.168.0.1", arg0);
            assertEquals(9205, arg1);
            return null;
        }).when(cli).tryConnect(any(), any());

        cli.main(new String[] { "--host", "192.168.0.1", "--port", "9205" }, terminal);
        assertThat(terminal.getErrorOutput(), emptyString());
    }

}
