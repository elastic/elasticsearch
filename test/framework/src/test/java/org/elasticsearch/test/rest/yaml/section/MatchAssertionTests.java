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

import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class MatchAssertionTests extends ESTestCase  {

    public void testNull() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", null);
            matchAssertion.doAssert(null, null);
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert("non-null", null));
        }
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", "non-null");
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert(null, "non-null"));
        }
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", "/exp/");
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert(null, "/exp/"));
        }
    }

    public void testNullInMap() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", singletonMap("a", null));
        matchAssertion.doAssert(singletonMap("a", null), matchAssertion.getExpectedValue());
        AssertionError e = expectThrows(AssertionError.class, () ->
            matchAssertion.doAssert(emptyMap(), matchAssertion.getExpectedValue()));
        assertThat(e.getMessage(), containsString("expected [null] but not found"));
    }
}
