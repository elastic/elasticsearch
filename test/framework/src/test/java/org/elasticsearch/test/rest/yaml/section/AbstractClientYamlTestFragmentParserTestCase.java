/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.After;

import static org.hamcrest.Matchers.nullValue;

/**
 * Superclass for tests that parse parts of the test suite.
 */
public abstract class AbstractClientYamlTestFragmentParserTestCase extends ESTestCase {
    protected XContentParser parser;

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        // test may be skipped so we did not create a parser instance
        if (parser != null) {
            //next token can be null even in the middle of the document (e.g. with "---"), but not too many consecutive times
            assertThat(parser.currentToken(), nullValue());
            assertThat(parser.nextToken(), nullValue());
            assertThat(parser.nextToken(), nullValue());
            parser.close();
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return ExecutableSection.XCONTENT_REGISTRY;
    }
}
