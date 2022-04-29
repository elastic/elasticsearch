/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.launcher.CliToolLauncher.getToolName;
import static org.hamcrest.Matchers.equalTo;

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
    }
}
