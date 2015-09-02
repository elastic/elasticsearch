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

package org.elasticsearch.rest.action.support;

import org.elasticsearch.common.Table;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.action.support.RestTable.buildDisplayHeaders;
import static org.hamcrest.Matchers.*;

public class RestTableTests extends ESTestCase {

    private Table table = new Table();
    private FakeRestRequest restRequest = new FakeRestRequest();

    @Before
    public void setup() {
        table.startHeaders();
        table.addCell("bulk.foo", "alias:f;desc:foo");
        table.addCell("bulk.bar", "alias:b;desc:bar");
        // should be matched as well due to the aliases
        table.addCell("aliasedBulk", "alias:bulkWhatever;desc:bar");
        table.addCell("aliasedSecondBulk", "alias:foobar,bulkolicious,bulkotastic;desc:bar");
        // no match
        table.addCell("unmatched", "alias:un.matched;desc:bar");
        // invalid alias
        table.addCell("invalidAliasesBulk", "alias:,,,;desc:bar");
        table.endHeaders();
    }

    @Test
    public void testThatDisplayHeadersSupportWildcards() throws Exception {
        restRequest.params().put("h", "bulk*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    @Test
    public void testThatDisplayHeadersAreNotAddedTwice() throws Exception {
        restRequest.params().put("h", "nonexistent,bulk*,bul*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    private List<String> getHeaderNames(List<RestTable.DisplayHeader> headers) {
        List<String> headerNames = new ArrayList<>();
        for (RestTable.DisplayHeader header : headers) {
            headerNames.add(header.name);
        }

        return headerNames;
    }
}
