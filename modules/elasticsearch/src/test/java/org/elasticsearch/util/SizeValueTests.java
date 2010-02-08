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

package org.elasticsearch.util;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SizeValueTests {

    @Test public void testSimple() {
        assertThat(SizeUnit.BYTES.toBytes(10), is(new SizeValue(10, SizeUnit.BYTES).bytes()));
        assertThat(SizeUnit.KB.toKB(10), is(new SizeValue(10, SizeUnit.KB).kb()));
        assertThat(SizeUnit.MB.toMB(10), is(new SizeValue(10, SizeUnit.MB).mb()));
        assertThat(SizeUnit.GB.toGB(10), is(new SizeValue(10, SizeUnit.GB).gb()));
    }

    @Test public void testToString() {
        assertThat("10b", is(new SizeValue(10, SizeUnit.BYTES).toString()));
        assertThat("1.5kb", is(new SizeValue((long) (1024 * 1.5), SizeUnit.BYTES).toString()));
        assertThat("1.5mb", is(new SizeValue((long) (1024 * 1.5), SizeUnit.KB).toString()));
        assertThat("1.5gb", is(new SizeValue((long) (1024 * 1.5), SizeUnit.MB).toString()));
        assertThat("1536gb", is(new SizeValue((long) (1024 * 1.5), SizeUnit.GB).toString()));
    }
}
