/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class TestScopeResolverTests extends ESTestCase {

    public void testScopeResolverServerClass() {
        var testBuildInfo = new TestBuildInfo(
            "server",
            List.of(new TestBuildInfoLocation("org/elasticsearch/Build.class", "org.elasticsearch.server"))
        );
        var resolver = TestScopeResolver.createScopeResolver(testBuildInfo, List.of());

        var scope = resolver.apply(Plugin.class);
        assertThat(scope.componentName(), is("(server)"));
        assertThat(scope.moduleName(), is("org.elasticsearch.server"));
    }

    public void testScopeResolverInternalClass() {
        var testBuildInfo = new TestBuildInfo(
            "server",
            List.of(new TestBuildInfoLocation("org/elasticsearch/Build.class", "org.elasticsearch.server"))
        );
        var testOwnBuildInfo = new TestBuildInfo(
            "test-component",
            List.of(new TestBuildInfoLocation("org/elasticsearch/bootstrap/TestBuildInfoParserTests.class", "test-module-name"))
        );
        var resolver = TestScopeResolver.createScopeResolver(testBuildInfo, List.of(testOwnBuildInfo));

        var scope = resolver.apply(this.getClass());
        assertThat(scope.componentName(), is("test-component"));
        assertThat(scope.moduleName(), is("test-module-name"));
    }
}
