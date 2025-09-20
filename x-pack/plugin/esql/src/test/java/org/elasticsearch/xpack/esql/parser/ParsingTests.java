/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ParsingTests extends AbstractStatementParserTests {

  public void testLoadResultParsesQuotedId() {
    var plan = statement("load_result \"FmNJRUZ1\"");
    assertThat(plan.getClass().getSimpleName(), equalTo("LoadResult"));
  }

  public void testLoadResultParsesUnquotedIsError() {
    expectError("load_result FmNJRUZ1", "token recognition error");
  }
}
