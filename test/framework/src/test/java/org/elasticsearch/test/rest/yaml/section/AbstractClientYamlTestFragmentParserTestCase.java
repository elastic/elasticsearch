/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
