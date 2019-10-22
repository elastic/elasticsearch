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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class AssertBusySectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testParseAssertBusySection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "  - max_wait_time: \"12s\"\n" +
            "  - do:\n" +
            "      index1:\n" +
            "        index:  test_1\n" +
            "        type:   test\n" +
            "        id:     1\n" +
            "        body:   { \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }\n" +
            "  - match: { field: 10 }\n" +
            "  - lt: { field: 10 }\n" +
            "  - lte: { field: 9 }\n" +
            "  - gt: { field: 8 }\n" +
            "  - gte: { field: 7 }\n" +
            "  - is_true: field1\n" +
            "  - is_false: field2\n"
        );

        AssertBusySection assertBusySection = AssertBusySection.parse(parser);

        assertThat(assertBusySection, notNullValue());
        assertThat(assertBusySection.getLocation(), notNullValue());
        assertThat(assertBusySection.getMaxWaitTime(), equalTo(TimeValue.timeValueSeconds(12L)));
        assertThat(assertBusySection.getDoSection(), instanceOf(DoSection.class));
        assertThat(assertBusySection.getDoSection().getApiCallSection().getApi(), equalTo("index1"));
        assertThat(assertBusySection.getAssertions().size(), equalTo(7));
        assertThat(assertBusySection.getAssertions().get(0), instanceOf(MatchAssertion.class));
        assertThat(assertBusySection.getAssertions().get(1), instanceOf(LessThanAssertion.class));
        assertThat(assertBusySection.getAssertions().get(2), instanceOf(LessThanOrEqualToAssertion.class));
        assertThat(assertBusySection.getAssertions().get(3), instanceOf(GreaterThanAssertion.class));
        assertThat(assertBusySection.getAssertions().get(4), instanceOf(GreaterThanEqualToAssertion.class));
        assertThat(assertBusySection.getAssertions().get(5), instanceOf(IsTrueAssertion.class));
        assertThat(assertBusySection.getAssertions().get(6), instanceOf(IsFalseAssertion.class));
    }
}
