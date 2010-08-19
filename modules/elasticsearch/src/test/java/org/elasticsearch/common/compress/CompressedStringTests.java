/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.compress;

import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class CompressedStringTests {

    @Test public void simpleTests() throws IOException {
        String str = "this is a simple string";
        CompressedString cstr = new CompressedString(str);
        assertThat(cstr.string(), equalTo(str));
        assertThat(new CompressedString(str), equalTo(cstr));

        String str2 = "this is a simple string 2";
        CompressedString cstr2 = new CompressedString(str2);
        assertThat(cstr2.string(), not(equalTo(str)));
        assertThat(new CompressedString(str2), not(equalTo(cstr)));
        assertThat(new CompressedString(str2), equalTo(cstr2));
    }
}
