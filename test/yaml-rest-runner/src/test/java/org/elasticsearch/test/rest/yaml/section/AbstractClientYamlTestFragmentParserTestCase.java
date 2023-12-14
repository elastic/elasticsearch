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
import org.hamcrest.Matcher;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.not;
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

    protected static SkipSectionContext versionContext(Version version) {
        return new SkipSectionContext() {
            @Override
            public boolean clusterIsRunningOs(String osName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean clusterHasFeature(String featureId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean clusterVersionInRange(VersionRange range) {
                return range.contains(version);
            }
        };
    }

    private static class TestSkipSectionContext implements SkipSectionContext {
        List<String> operatingSystems = new ArrayList<>();
        List<String> clusterFeatures = new ArrayList<>();
        List<VersionRange> versionRanges = new ArrayList<>();

        @Override
        public boolean clusterIsRunningOs(String osName) {
            operatingSystems.add(osName);
            return false;
        }

        @Override
        public boolean clusterHasFeature(String featureId) {
            clusterFeatures.add(featureId);
            return false;
        }

        @Override
        public boolean clusterVersionInRange(VersionRange range) {
            versionRanges.add(range);
            return false;
        }
    }

    protected void assertIsVersionCheck(SkipSection skipSection, boolean isVersionCheck) {
        var context = new TestSkipSectionContext();
        skipSection.skip(context);
        if (isVersionCheck) {
            assertThat(context.versionRanges, not(emptyIterable()));
        } else {
            assertThat(context.versionRanges, emptyIterable());
        }
    }

    protected void assertOperatingSystems(SkipSection skipSection, Matcher<Iterable<? extends String>> matcher) {
        var context = new TestSkipSectionContext();
        skipSection.skip(context);
        assertThat(context.operatingSystems, matcher);
    }
}
