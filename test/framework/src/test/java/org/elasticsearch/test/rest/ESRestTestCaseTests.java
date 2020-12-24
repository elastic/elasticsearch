/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest;

import org.elasticsearch.test.ESTestCase;

import java.util.regex.Matcher;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ESRestTestCaseTests extends ESTestCase {

    public void testIgnoreMatchMultipleTemplatesPattern() {
        String input = "index [test_index] matches multiple legacy templates [global, prevent-bwc-deprecation-template], " +
            "composable templates will only match a single template";
        Matcher matcher = ESRestTestCase.CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES.matcher(input);
        assertThat(matcher.matches(), is(true));
        assertThat(matcher.group(1), equalTo("test_index"));
        assertThat(matcher.group(2), equalTo("global, prevent-bwc-deprecation-template"));

        input = "index template [1] has index patterns [logs-*] matching patterns from existing older templates [global] " +
            "with patterns (global => [*]); this template [1] will take precedence during new index creation";
        matcher = ESRestTestCase.PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES.matcher(input);
        assertThat(matcher.matches(), is(true));
        assertThat(matcher.group(1), equalTo("1"));
        assertThat(matcher.group(2), equalTo("logs-*"));
        assertThat(matcher.group(3), equalTo("global"));
        assertThat(matcher.group(4), equalTo("global => [*]"));
        assertThat(matcher.group(5), equalTo("1"));
    }

}
