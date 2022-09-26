/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.launcher.CliToolLauncher.createShutdownHook;
import static org.elasticsearch.launcher.CliToolLauncher.getToolName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CliToolLauncherTests extends ESTestCase {

    public void testCliNameSysprop() {
        assertThat(getToolName(Map.of("cli.name", "mycli")), equalTo("mycli"));
    }

    public void testScriptNameSysprop() {
        var sysprops = Map.of("cli.name", "", "cli.script", "/foo/bar/elasticsearch-mycli", "os.name", "Linux");
        assertThat(getToolName(sysprops), equalTo("mycli"));
    }

    public void testScriptNameSyspropWindows() {
        var sysprops = Map.of("cli.name", "", "cli.script", "C:\\foo\\bar\\elasticsearch-mycli.bat", "os.name", "Windows XP");
        assertThat(getToolName(sysprops), equalTo("mycli"));
        sysprops = Map.of("cli.name", "", "cli.script", "C:\\foo\\bar\\elasticsearch-mycli", "os.name", "Windows XP");
        assertThat(getToolName(sysprops), equalTo("mycli"));
    }

    public void testShutdownHook() {
        MockTerminal terminal = MockTerminal.create();
        AtomicBoolean closeCalled = new AtomicBoolean();
        Closeable toClose = () -> { closeCalled.set(true); };
        Thread hook = createShutdownHook(terminal, toClose);
        hook.run();
        assertThat(closeCalled.get(), is(true));
        assertThat(terminal.getErrorOutput(), emptyString());
    }

    public void testShutdownHookError() {
        MockTerminal terminal = MockTerminal.create();
        Closeable toClose = () -> { throw new IOException("something bad happened"); };
        Thread hook = createShutdownHook(terminal, toClose);
        hook.run();
        assertThat(terminal.getErrorOutput(), containsString("something bad happened"));
        // ensure that we dump the stack trace too
        assertThat(terminal.getErrorOutput(), containsString("at org.elasticsearch.launcher.CliToolLauncherTests.lambda"));
    }
}
