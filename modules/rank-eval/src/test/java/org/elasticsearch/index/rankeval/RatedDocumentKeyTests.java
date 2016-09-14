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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RatedDocumentKeyTests extends ESTestCase {

    public void testEqualsAndHash() throws IOException {
        String index = randomAsciiOfLengthBetween(0, 10);
        String type = randomAsciiOfLengthBetween(0, 10);
        String docId = randomAsciiOfLengthBetween(0, 10);

        RatedDocumentKey testItem = new RatedDocumentKey(index, type, docId);

        assertFalse("key is equal to null", testItem.equals(null));
        assertFalse("key is equal to incompatible type", testItem.equals(""));
        assertTrue("key is not equal to self", testItem.equals(testItem));
        assertThat("same key's hashcode returns different values if called multiple times", testItem.hashCode(),
                equalTo(testItem.hashCode()));

        RatedDocumentKey mutation;
        switch (randomIntBetween(0, 2)) {
        case 0:
            mutation = new RatedDocumentKey(testItem.getIndex() + "_foo", testItem.getType(), testItem.getDocID());
            break;
        case 1:
            mutation = new RatedDocumentKey(testItem.getIndex(), testItem.getType()  + "_foo", testItem.getDocID());
            break;
        case 2:
            mutation = new RatedDocumentKey(testItem.getIndex(), testItem.getType(), testItem.getDocID()  + "_foo");
            break;
        default:
            throw new IllegalStateException("The test should only allow three parameters mutated");
        }

        assertThat("different keys should not be equal", mutation, not(equalTo(testItem)));

        RatedDocumentKey secondEqualKey = new RatedDocumentKey(index, type, docId);
        assertTrue("key is not equal to its copy", testItem.equals(secondEqualKey));
        assertTrue("equals is not symmetric", secondEqualKey.equals(testItem));
        assertThat("key copy's hashcode is different from original hashcode", secondEqualKey.hashCode(),
                equalTo(testItem.hashCode()));
    }
}
