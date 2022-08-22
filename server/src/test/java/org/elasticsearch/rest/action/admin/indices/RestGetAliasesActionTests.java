/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;

public class RestGetAliasesActionTests extends ESTestCase {

    // # Assumes the following setup
    // curl -X PUT "localhost:9200/index" -H "Content-Type: application/json" -d'
    // {
    // "aliases": {
    // "foo": {},
    // "foobar": {}
    // }
    // }'

    public void testBareRequest() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final AliasMetadata foobarAliasMetadata = AliasMetadata.builder("foobar").build();
        final AliasMetadata fooAliasMetadata = AliasMetadata.builder("foo").build();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            false,
            new String[0],
            Map.of("index", Arrays.asList(fooAliasMetadata, foobarAliasMetadata)),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{},\"foobar\":{}}}}"));
    }

    public void testSimpleAliasWildcardMatchingNothing() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "baz*" },
            Map.of(),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testMultipleAliasWildcardsSomeMatching() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foobar").build();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "baz*", "foobar*" },
            Map.of("index", List.of(aliasMetadata)),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foobar\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeAll() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foob*", "-foo*" },
            Map.of(),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSome() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foo*", "-foob*" },
            Map.of("index", List.of(aliasMetadata)),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSomeAndExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        final String[] aliasPattern;
        if (randomBoolean()) {
            aliasPattern = new String[] { "missing", "foo*", "-foob*" };
        } else {
            aliasPattern = new String[] { "foo*", "-foob*", "missing" };
        }

        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            aliasPattern,
            Map.of("index", List.of(aliasMetadata)),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(NOT_FOUND));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(
            restResponse.content().utf8ToString(),
            equalTo("{\"error\":\"alias [missing] missing\",\"status\":404,\"index\":{\"aliases\":{\"foo\":{}}}}")
        );
    }

    public void testAliasWildcardsExcludeExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foo", "foofoo", "-foo*" },
            Map.of(),
            Map.of(),
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }
}
