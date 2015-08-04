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
package org.elasticsearch.common.unit;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class SizeValueTests extends ESTestCase {

    @Test
    public void testThatConversionWorks() {
        SizeValue sizeValue = new SizeValue(1000);
        assertThat(sizeValue.kilo(), is(1l));
        assertThat(sizeValue.toString(), is("1k"));

        sizeValue = new SizeValue(1000, SizeUnit.KILO);
        assertThat(sizeValue.singles(), is(1000000L));
        assertThat(sizeValue.toString(), is("1m"));

        sizeValue = new SizeValue(1000, SizeUnit.MEGA);
        assertThat(sizeValue.singles(), is(1000000000L));
        assertThat(sizeValue.toString(), is("1g"));

        sizeValue = new SizeValue(1000, SizeUnit.GIGA);
        assertThat(sizeValue.singles(), is(1000000000000L));
        assertThat(sizeValue.toString(), is("1t"));

        sizeValue = new SizeValue(1000, SizeUnit.TERA);
        assertThat(sizeValue.singles(), is(1000000000000000L));
        assertThat(sizeValue.toString(), is("1p"));

        sizeValue = new SizeValue(1000, SizeUnit.PETA);
        assertThat(sizeValue.singles(), is(1000000000000000000L));
        assertThat(sizeValue.toString(), is("1000p"));
    }

    @Test
    public void testThatParsingWorks() {
        assertThat(SizeValue.parseSizeValue("1k").toString(), is(new SizeValue(1000).toString()));
        assertThat(SizeValue.parseSizeValue("1p").toString(), is(new SizeValue(1, SizeUnit.PETA).toString()));
        assertThat(SizeValue.parseSizeValue("1G").toString(), is(new SizeValue(1, SizeUnit.GIGA).toString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatNegativeValuesThrowException() {
        new SizeValue(-1);
    }
}
