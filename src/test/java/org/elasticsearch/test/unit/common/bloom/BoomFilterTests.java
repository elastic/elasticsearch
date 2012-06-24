/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.unit.common.bloom;

import com.google.common.base.Charsets;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.bloom.BloomFilterFactory;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class BoomFilterTests {

    @Test
    public void testSimpleOps() {
        BloomFilter filter = BloomFilterFactory.getFilter(10, 15);
        filter.add(wrap("1"), 0, wrap("1").length);
        assertThat(filter.isPresent(wrap("1"), 0, wrap("1").length), equalTo(true));
        assertThat(filter.isPresent(wrap("2"), 0, wrap("2").length), equalTo(false));
        filter.add(wrap("2"), 0, wrap("2").length);
        assertThat(filter.isPresent(wrap("1"), 0, wrap("1").length), equalTo(true));
        assertThat(filter.isPresent(wrap("2"), 0, wrap("2").length), equalTo(true));
    }

    private byte[] wrap(String key) {
        return key.getBytes(Charsets.UTF_8);
    }
}