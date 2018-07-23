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

package org.elasticsearch.dissect;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DissectPairTests extends ESTestCase {

    public void testNoModifierSameOrder(){
        String keyName1 = randomAlphaOfLengthBetween(1, 10);
        String keyName2 = randomAlphaOfLengthBetween(1, 10);
        String value = randomAlphaOfLengthBetween(1, 10);
        DissectPair pair1 = new DissectPair(new DissectKey(keyName1), value);
        DissectPair pair2 = new DissectPair(new DissectKey(keyName2), value);
        assertThat(pair1.compareTo(pair2), equalTo(0));
        assertThat(pair2.compareTo(pair1), equalTo(0));
    }

    public void testAppendDifferentOrder(){
        String keyName = randomAlphaOfLengthBetween(1, 10);
        String value = randomAlphaOfLengthBetween(1, 10);
        int length = randomIntBetween(1, 100);
        DissectPair pair1 = new DissectPair(new DissectKey("+" + keyName + "/" + length), value);
        DissectPair pair2 = new DissectPair(new DissectKey("+" + keyName + "/" + length + 1), value);
        assertThat(pair1.compareTo(pair2), equalTo(-1));
        assertThat(pair2.compareTo(pair1), equalTo(1));
    }
    public void testAppendWithImplicitZeroOrder(){
        String keyName = randomAlphaOfLengthBetween(1, 10);
        String value = randomAlphaOfLengthBetween(1, 10);
        int length = randomIntBetween(1, 100);
        DissectPair pair1 = new DissectPair(new DissectKey("keyName"), value);
        DissectPair pair2 = new DissectPair(new DissectKey("+" + keyName + "/" + length), value);
        assertThat(pair1.compareTo(pair2), equalTo(-1));
        assertThat(pair2.compareTo(pair1), equalTo(1));
    }

    public void testAppendSameOrder(){
        String keyName = randomAlphaOfLengthBetween(1, 10);
        String value = randomAlphaOfLengthBetween(1, 10);
        int length = randomIntBetween(1, 100);
        DissectPair pair1 = new DissectPair(new DissectKey("+" + keyName + "/" + length), value);
        DissectPair pair2 = new DissectPair(new DissectKey("+" + keyName + "/" + length), value);
        assertThat(pair1.compareTo(pair2), equalTo(0));
        assertThat(pair2.compareTo(pair1), equalTo(0));
    }

    public void testFieldNameOrder(){
        String keyName = randomAlphaOfLengthBetween(1, 10);
        String value = randomAlphaOfLengthBetween(1, 10);
        DissectPair pair1 = new DissectPair(new DissectKey("?" + keyName), value);
        DissectPair pair2 = new DissectPair(new DissectKey("&" + keyName), value);
        assertThat(pair1.compareTo(pair2), equalTo(-1));
        assertThat(pair2.compareTo(pair1), equalTo(1));
    }
}
