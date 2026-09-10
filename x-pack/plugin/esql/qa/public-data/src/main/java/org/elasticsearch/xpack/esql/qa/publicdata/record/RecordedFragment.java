/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.record;

/**
 * One captured actual-result table, rendered in csv-spec expected-table syntax. Strictly a
 * mismatch diagnostic (what ES|QL actually returned, to hold next to the oracle's answer at the
 * stop-and-ask gate) — never a source of expected values, and never checked in.
 *
 * @param testName     the spec test that produced it
 * @param variantLabel the variant it ran against
 * @param renderedTable the actual results in csv-spec table syntax
 */
public record RecordedFragment(String testName, String variantLabel, String renderedTable) {}
