/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.common.CommandLineParser.ShellType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CommandLineParserProcessorFactoryTests extends ESTestCase {

    public void testCreateWithDefaults() throws Exception {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "process.command_line");

        String tag = randomAlphaOfLength(10);
        CommandLineParserProcessor processor = factory.create(null, tag, null, config, null);

        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getField(), equalTo("process.command_line"));
        assertThat(processor.getTargetField(), equalTo("process.command_line"));
        assertThat(processor.getShell(), equalTo(ShellType.BASH));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testCreateWithAllOptions() throws Exception {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "cmdline");
        config.put("target_field", "args");
        config.put("shell", "powershell");
        config.put("ignore_missing", true);

        String tag = randomAlphaOfLength(10);
        CommandLineParserProcessor processor = factory.create(null, tag, null, config, null);

        assertThat(processor.getField(), equalTo("cmdline"));
        assertThat(processor.getTargetField(), equalTo("args"));
        assertThat(processor.getShell(), equalTo(ShellType.POWERSHELL));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCreateWithShellCmd() throws Exception {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "cmdline");
        config.put("shell", "cmd");

        CommandLineParserProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getShell(), equalTo(ShellType.CMD));
    }

    public void testCreateWithShellSh() throws Exception {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "cmdline");
        config.put("shell", "sh");

        CommandLineParserProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getShell(), equalTo(ShellType.BASH));
    }

    public void testCreateMissingField() {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateInvalidShell() {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "cmdline");
        config.put("shell", "zsh");

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), containsString("[shell]"));
        assertThat(e.getMessage(), containsString("unsupported shell type [zsh]"));
    }
}
