/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/**
 * The inline {@code EXTERNAL} command no longer exists: it must not parse in snapshot or release builds.
 * External data is reached through {@code FROM <dataset>}.
 */
public class ExternalCommandRemovedTests extends AbstractStatementParserTests {

    public void testExternalCommandDoesNotParse() {
        for (String query : new String[] {
            "EXTERNAL \"s3://bucket/table\"",
            "EXTERNAL \"s3://bucket/table\" WITH { \"use_cache\": true }",
            "EXTERNAL \"s3://bucket/table\" | WHERE age > 25 | LIMIT 10" }) {
            ParsingException pe = expectThrows(ParsingException.class, () -> query(query));
            assertThat(query, pe.getMessage(), containsString("mismatched input 'EXTERNAL'"));
        }
    }

    public void testExternalCommandDoesNotParseWithDefaultParser() {
        EsqlParser parser = new EsqlParser(new EsqlConfig(TEST_FUNCTION_REGISTRY));
        ParsingException pe = expectThrows(ParsingException.class, () -> parser.createStatement("EXTERNAL \"s3://bucket/table\""));
        assertThat(pe.getMessage(), containsString("mismatched input 'EXTERNAL'"));
    }
}
