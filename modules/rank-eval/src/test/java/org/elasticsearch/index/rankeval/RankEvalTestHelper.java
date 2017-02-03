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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RankEvalTestHelper {

    public static <T> void testHashCodeAndEquals(T testItem, T mutation, T secondCopy) {
        assertFalse("testItem is equal to null", testItem.equals(null));
        assertFalse("testItem is equal to incompatible type", testItem.equals(""));
        assertTrue("testItem is not equal to self", testItem.equals(testItem));
        assertThat("same testItem's hashcode returns different values if called multiple times", testItem.hashCode(),
                equalTo(testItem.hashCode()));

        assertThat("different testItem should not be equal", mutation, not(equalTo(testItem)));

        assertNotSame("testItem copy is not same as original", testItem, secondCopy);
        assertTrue("testItem is not equal to its copy", testItem.equals(secondCopy));
        assertTrue("equals is not symmetric", secondCopy.equals(testItem));
        assertThat("testItem copy's hashcode is different from original hashcode", secondCopy.hashCode(),
                equalTo(testItem.hashCode()));
    }

    /**
     * Make a deep copy of an object by running it through a BytesStreamOutput
     * @param original the original object
     * @param reader a function able to create a new copy of this type
     * @return a new copy of the original object
     */
    public static <T extends Writeable> T copy(T original, Writeable.Reader<T> reader) throws IOException {
        return copy(original, reader, new NamedWriteableRegistry(Collections.emptyList()));
    }

    /**
     * Make a deep copy of an object by running it through a BytesStreamOutput
     * @param original the original object
     * @param reader a function able to create a new copy of this type
     * @param namedWriteableRegistry must be non-empty if the object itself or nested object implement {@link NamedWriteable}
     * @return a new copy of the original object
     */
    public static <T extends Writeable> T copy(T original, Writeable.Reader<T> reader, NamedWriteableRegistry namedWriteableRegistry)
            throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return reader.read(in);
            }
        }
    }
}
