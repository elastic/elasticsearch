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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class SuggestingErrorOnUnknownTests extends ESTestCase {
    private String errorMessage(String unknownField, String... candidates) {
        return new SuggestingErrorOnUnknown().errorMessage("test", unknownField, Arrays.asList(candidates));
    }

    public void testNoCandidates() {
        assertThat(errorMessage("foo"), equalTo("[test] unknown field [foo]"));
    }
    public void testBadCandidates() {
        assertThat(errorMessage("foo", "bar", "baz"), equalTo("[test] unknown field [foo]"));
    }
    public void testOneCandidate() {
        assertThat(errorMessage("foo", "bar", "fop"), equalTo("[test] unknown field [foo] did you mean [fop]?"));
    }
    public void testManyCandidate() {
        assertThat(errorMessage("foo", "bar", "fop", "fou", "baz"),
                equalTo("[test] unknown field [foo] did you mean any of [fop, fou]?"));
    }
}
