/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest;

import org.elasticsearch.test.ESTestCase;

import java.util.regex.Matcher;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ESRestTestCaseTests extends ESTestCase {

    public void testIgnoreMatchMultipleTemplatesPattern() {
        String input = "index [test_index] matches multiple legacy templates [global, prevent-bwc-deprecation-template], "
            + "composable templates will only match a single template";
        Matcher matcher = ESRestTestCase.CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES.matcher(input);
        assertThat(matcher.matches(), is(true));
        assertThat(matcher.group(1), equalTo("test_index"));
        assertThat(matcher.group(2), equalTo("global, prevent-bwc-deprecation-template"));

        input = "index template [1] has index patterns [logs-*] matching patterns from existing older templates [global] "
            + "with patterns (global => [*]); this template [1] will take precedence during new index creation";
        matcher = ESRestTestCase.PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES.matcher(input);
        assertThat(matcher.matches(), is(true));
        assertThat(matcher.group(1), equalTo("1"));
        assertThat(matcher.group(2), equalTo("logs-*"));
        assertThat(matcher.group(3), equalTo("global"));
        assertThat(matcher.group(4), equalTo("global => [*]"));
        assertThat(matcher.group(5), equalTo("1"));
    }

}
