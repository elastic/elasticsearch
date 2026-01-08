/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.Before;

import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for the ESQL WORKFLOW command.
 *
 * NOTE: These tests require a mock Kibana workflow service to be available.
 * For the POC, tests that require actual workflow execution are marked with
 * @AwaitsFix or skipped until the mock service is implemented.
 *
 * Tests cover:
 * - Parsing validation (works without Kibana)
 * - Error handling for missing workflows
 * - Basic syntax validation
 */
public class WorkflowIT extends AbstractEsqlIntegTestCase {

    private static final String TEST_INDEX = "test_workflow";

    @Before
    public void setupIndex() {
        assumeTrue("WORKFLOW requires corresponding capability", EsqlCapabilities.Cap.WORKFLOW.isEnabled());
        createAndPopulateTestIndex();
    }

    private void createAndPopulateTestIndex() {
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(TEST_INDEX)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "alert_message", "type=text", "severity", "type=keyword");
        assertAcked(createRequest.get());

        var bulkRequest = client().prepareBulk();
        for (int i = 1; i <= 6; i++) {
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).id(String.valueOf(i))
                    .source(
                        "id",
                        i,
                        "alert_message",
                        "Alert " + i + " detected",
                        "severity",
                        i % 3 == 0 ? "high" : (i % 2 == 0 ? "medium" : "low")
                    )
            );
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ensureYellow(TEST_INDEX);
    }

    // ============================================
    // Parsing and Syntax Tests (No Kibana Required)
    // ============================================

    /**
     * Test that the WORKFLOW command with inputs parses correctly.
     * This test validates the grammar without executing the workflow.
     *
     * Grammar: WORKFLOW workflowId=string WITH LP workflowInputs RP (AS targetField)?
     */
    public void testWorkflowCommandParsing() {
        // Basic workflow command with required WITH clause - should parse but fail at execution without Kibana
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WORKFLOW "test-workflow" WITH (message = alert_message)
            | LIMIT 1
            """, TEST_INDEX);

        // The query should fail during execution (no Kibana), not during parsing
        var error = expectThrows(Exception.class, () -> run(query));
        // The error should NOT be a ParsingException (parsing should succeed)
        assertFalse("Should not be a parsing error", error instanceof ParsingException);
    }

    /**
     * Test WORKFLOW with multiple named inputs parses correctly.
     */
    public void testWorkflowWithMultipleInputsParsing() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WORKFLOW "process-alert" WITH (message = alert_message, level = severity)
            | LIMIT 1
            """, TEST_INDEX);

        // Should fail during execution, not parsing
        var error = expectThrows(Exception.class, () -> run(query));
        assertFalse("Should not be a parsing error", error instanceof ParsingException);
    }

    /**
     * Test WORKFLOW with AS target field parses correctly.
     */
    public void testWorkflowWithTargetFieldParsing() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WORKFLOW "process-alert" WITH (message = alert_message) AS result
            | LIMIT 1
            """, TEST_INDEX);

        // Should fail during execution, not parsing
        var error = expectThrows(Exception.class, () -> run(query));
        assertFalse("Should not be a parsing error", error instanceof ParsingException);
    }

    /**
     * Test WORKFLOW with full syntax (inputs and target field) parses correctly.
     */
    public void testWorkflowFullSyntaxParsing() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WHERE severity == "high"
            | WORKFLOW "enrich-alert" WITH (msg = alert_message, sev = severity) AS enriched
            | KEEP id, enriched
            | LIMIT 5
            """, TEST_INDEX);

        // Should fail during execution, not parsing
        var error = expectThrows(Exception.class, () -> run(query));
        assertFalse("Should not be a parsing error", error instanceof ParsingException);
    }

    // ============================================
    // Error Handling Tests
    // ============================================

    /**
     * Test that WORKFLOW without WITH clause produces parsing error.
     */
    public void testWorkflowInvalidSyntaxMissingWithClause() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WORKFLOW "test-workflow"
            | LIMIT 1
            """, TEST_INDEX);

        var error = expectThrows(ParsingException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("mismatched input"));
    }

    /**
     * Test that WORKFLOW without id produces parsing error.
     */
    public void testWorkflowInvalidSyntaxMissingId() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WORKFLOW
            | LIMIT 1
            """, TEST_INDEX);

        var error = expectThrows(ParsingException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("mismatched input"));
    }

    /**
     * Test that WORKFLOW without inputs produces parsing error.
     */
    public void testWorkflowInvalidSyntaxEmptyInputs() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WORKFLOW "test" WITH ()
            | LIMIT 1
            """, TEST_INDEX);

        var error = expectThrows(ParsingException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("mismatched input"));
    }
}
