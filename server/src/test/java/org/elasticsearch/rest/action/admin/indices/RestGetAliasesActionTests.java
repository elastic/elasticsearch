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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.hamcrest.Matchers.equalTo;

public class RestGetAliasesActionTests extends ESTestCase {

//    # Assumes the following setup
//    curl -X PUT "localhost:9200/index" -H "Content-Type: application/json" -d'
//    {
//      "aliases": {
//        "foo": {},
//        "foobar": {}
//      }
//    }'

    public void testBareRequest() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final AliasMetadata foobarAliasMetadata = AliasMetadata.builder("foobar").build();
        final AliasMetadata fooAliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(fooAliasMetadata, foobarAliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(false, new String[0], openMapBuilder.build(),
                xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{},\"foobar\":{}}}}"));
    }

    public void testSimpleAliasWildcardMatchingNothing() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, new String[] { "baz*" }, openMapBuilder.build(),
                xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testMultipleAliasWildcardsSomeMatching() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foobar").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, new String[] { "baz*", "foobar*" },
                openMapBuilder.build(), xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foobar\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeAll() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, new String[] { "foob*", "-foo*" },
                openMapBuilder.build(), xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSome() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, new String[] { "foo*", "-foob*" },
                openMapBuilder.build(), xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSomeAndExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final String[] aliasPattern;
        if (randomBoolean()) {
            aliasPattern = new String[] { "missing", "foo*", "-foob*" };
        } else {
            aliasPattern = new String[] { "foo*", "-foob*", "missing" };
        }

        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, aliasPattern, openMapBuilder.build(),
                xContentBuilder);
        assertThat(restResponse.status(), equalTo(NOT_FOUND));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(),
                equalTo("{\"error\":\"alias [missing] missing\",\"status\":404,\"index\":{\"aliases\":{\"foo\":{}}}}"));
    }

    public void testAliasWildcardsExcludeExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final ImmutableOpenMap.Builder<String, List<AliasMetadata>> openMapBuilder = ImmutableOpenMap.builder();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, new String[] { "foo", "foofoo", "-foo*" },
                openMapBuilder.build(), xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }
}
