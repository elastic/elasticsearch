/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;

/**
 * Test that generates PromQL Kibana definition JSON files.
 * <p>
 * Run with:
 * <pre>
 * ./gradlew :x-pack:plugin:esql:test --tests "PromqlKibanaDefinitionGeneratorTests" -DgenerateDocs=write
 * </pre>
 * <p>
 * Generated files will be written to:
 * <pre>
 * $TMPDIR/promql/kibana/definition/*
 * </pre>
 */
public class PromqlKibanaDefinitionGeneratorTests extends ESTestCase {
    public void testGenerateDefinitions() throws Exception {
        DocsV3Support.Callbacks callbacks = DocsV3Support.callbacksFromSystemProperty();
        PromqlDocsSupport.entrypoint(callbacks);
    }
}
