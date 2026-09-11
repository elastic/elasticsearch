/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.ingest.common.CommandLineParser.ShellType;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CommandLineParserProcessorTests extends ESTestCase {

    public void testParseBash() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", "ls -la '/tmp/my dir'");
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.BASH, false);
        processor.execute(doc);

        assertThat(doc.getFieldValue("args", List.class), equalTo(List.of("ls", "-la", "/tmp/my dir")));
    }

    public void testParseCmd() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", "\"C:\\Program Files\\app.exe\" --flag \"hello world\"");
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.CMD, false);
        processor.execute(doc);

        assertThat(doc.getFieldValue("args", List.class), equalTo(List.of("C:\\Program Files\\app.exe", "--flag", "hello world")));
    }

    public void testParsePowerShell() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", "Write-Host 'it''s working'");
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(
            randomAlphaOfLength(10),
            null,
            "command_line",
            "args",
            ShellType.POWERSHELL,
            false
        );
        processor.execute(doc);

        assertThat(doc.getFieldValue("args", List.class), equalTo(List.of("Write-Host", "it's working")));
    }

    public void testTargetFieldSameAsSource() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", "echo hello world");
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(
            randomAlphaOfLength(10),
            null,
            "command_line",
            "command_line",
            ShellType.BASH,
            false
        );
        processor.execute(doc);

        assertThat(doc.getFieldValue("command_line", List.class), equalTo(List.of("echo", "hello", "world")));
    }

    public void testFieldNotFound() {
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "nonexistent", "args", ShellType.BASH, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), containsString("not present as part of path [nonexistent]"));
    }

    public void testNullValue() {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", null);
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.BASH, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [command_line] is null, cannot parse command line."));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", null);
        IngestDocument originalDoc = TestIngestDocument.withDefaultVersion(source);
        IngestDocument doc = new IngestDocument(originalDoc);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.BASH, true);
        processor.execute(doc);
        assertIngestDocument(originalDoc, doc);
    }

    public void testNonExistentFieldWithIgnoreMissing() throws Exception {
        IngestDocument originalDoc = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        IngestDocument doc = new IngestDocument(originalDoc);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "nonexistent", "args", ShellType.BASH, true);
        processor.execute(doc);
        assertIngestDocument(originalDoc, doc);
    }

    public void testNonStringValue() {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", 42);
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.BASH, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), containsString("cannot be cast to [java.lang.String]"));
    }

    public void testGetType() {
        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.BASH, false);
        assertThat(processor.getType(), equalTo("command_line_args"));
    }

    public void testEmptyCommandLine() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("command_line", "");
        IngestDocument doc = TestIngestDocument.withDefaultVersion(source);

        Processor processor = new CommandLineParserProcessor(randomAlphaOfLength(10), null, "command_line", "args", ShellType.BASH, false);
        processor.execute(doc);

        assertThat(doc.getFieldValue("args", List.class), equalTo(Collections.emptyList()));
    }

    public void testFactoryCreate() throws Exception {
        CommandLineParserProcessor.Factory factory = new CommandLineParserProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "process.command_line");
        config.put("target_field", "process.args");
        config.put("shell", "cmd");
        config.put("ignore_missing", true);

        String tag = randomAlphaOfLength(10);
        CommandLineParserProcessor processor = factory.create(null, tag, null, config, null);

        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getField(), equalTo("process.command_line"));
        assertThat(processor.getTargetField(), equalTo("process.args"));
        assertThat(processor.getShell(), equalTo(ShellType.CMD));
        assertTrue(processor.isIgnoreMissing());
    }
}
