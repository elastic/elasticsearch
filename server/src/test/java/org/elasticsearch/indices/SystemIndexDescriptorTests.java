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

package org.elasticsearch.indices;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class SystemIndexDescriptorTests extends ESTestCase {

    public void testValidation() {
        {
            Exception ex = expectThrows(NullPointerException.class,
                () -> new SystemIndexDescriptor(null, randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor("", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(randomAlphaOfLength(10), randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*" + randomAlphaOfLength(10), randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
    }

    public void testOverlappingPatterns() {
        Random random = random();
        SystemIndexDescriptor broadPattern = new SystemIndexDescriptor(".a*c*", "test");
        SystemIndexDescriptor notOverlapping = new SystemIndexDescriptor(".bbbddd*", "test");
        SystemIndexDescriptor overlapping1 = new SystemIndexDescriptor(".ac*", "test");
        SystemIndexDescriptor overlapping2 = new SystemIndexDescriptor(".aaaabbbccc", "test");
        SystemIndexDescriptor overlapping3 = new SystemIndexDescriptor(".aaabb*cccddd*", "test");

        String broadPatternSource = randomAlphaOfLength(5);
        String otherSource = randomAlphaOfLength(6);
        Map<String, Collection<SystemIndexDescriptor>> descriptors = new HashMap<>();
        descriptors.put(broadPatternSource, Arrays.asList(broadPattern));
        descriptors.put(otherSource, Arrays.asList(notOverlapping, overlapping1, overlapping2, overlapping3));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> SystemIndexDescriptor.checkForOverlappingPatterns(descriptors));
        assertThat(exception.getMessage(), containsString("a system index descriptor [" + broadPattern +
            "] from plugin [" + broadPatternSource + "] overlaps with other system index descriptors:"));
        String fromPluginString = " from plugin [" + otherSource + "]";
        assertThat(exception.getMessage(), containsString(overlapping1.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping2.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping3.toString() + fromPluginString));
        assertThat(exception.getMessage(), not(containsString(notOverlapping.toString())));
    }

    /*
     * Some overlapping patterns will not be caught by the current overlapping-pattern detection. If this changes,
     * this may be a breaking change for the plugin API.
     */
    public void testPotentialCollisionsNotCaught() {
        Random random = random();
        SystemIndexDescriptor pattern1 = new SystemIndexDescriptor(".a*c", "test");
        SystemIndexDescriptor pattern2 = new SystemIndexDescriptor(".ab*", "test");

        String source1 = randomAlphaOfLength(5);
        String source2 = randomAlphaOfLength(6);
        Map<String, Collection<SystemIndexDescriptor>> descriptors = new HashMap<>();
        descriptors.put(source1, Arrays.asList(pattern1));
        descriptors.put(source2, Arrays.asList(pattern2));

        SystemIndexDescriptor.checkForOverlappingPatterns(descriptors);
    }
}
