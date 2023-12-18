/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
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
            // next token can be null even in the middle of the document (e.g. with "---"), but not too many consecutive times
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

    protected static SkipSectionContext versionOnlyContext(Version version) {
        return new SkipSectionContext() {
            @Override
            public boolean clusterIsRunningOs(String osName) {
                return false;
            }

            @Override
            public boolean clusterHasFeature(String featureId) {
                return true;
            }

            @Override
            public boolean clusterVersionInRange(VersionRange range) {
                return range.contains(version);
            }

        };
    }

    protected static SkipSectionContext neverSkipContext() {
        return new SkipSectionContext() {
            @Override
            public boolean clusterIsRunningOs(String osName) {
                return false;
            }

            @Override
            public boolean clusterHasFeature(String featureId) {
                return true;
            }

            @Override
            public boolean clusterVersionInRange(VersionRange range) {
                return false;
            }

        };
    }

    protected static SkipSectionContext osOnlyContext(String os) {
        return new SkipSectionContext() {
            @Override
            public boolean clusterIsRunningOs(String osName) {
                return osName.equals(os);
            }

            @Override
            public boolean clusterHasFeature(String featureId) {
                return true;
            }

            @Override
            public boolean clusterVersionInRange(VersionRange range) {
                return false;
            }
        };
    }
}
