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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ByteSizeValueTests extends ElasticsearchTestCase {

    @Test
    public void testActualPeta() {
        MatcherAssert.assertThat(new ByteSizeValue(4, ByteSizeUnit.PB).bytes(), equalTo(4503599627370496l));
    }

    @Test
    public void testActualTera() {
        MatcherAssert.assertThat(new ByteSizeValue(4, ByteSizeUnit.TB).bytes(), equalTo(4398046511104l));
    }

    @Test
    public void testActual() {
        MatcherAssert.assertThat(new ByteSizeValue(4, ByteSizeUnit.GB).bytes(), equalTo(4294967296l));
    }

    @Test
    public void testSimple() {
        assertThat(ByteSizeUnit.BYTES.toBytes(10), is(new ByteSizeValue(10, ByteSizeUnit.BYTES).bytes()));
        assertThat(ByteSizeUnit.KB.toKB(10), is(new ByteSizeValue(10, ByteSizeUnit.KB).kb()));
        assertThat(ByteSizeUnit.MB.toMB(10), is(new ByteSizeValue(10, ByteSizeUnit.MB).mb()));
        assertThat(ByteSizeUnit.GB.toGB(10), is(new ByteSizeValue(10, ByteSizeUnit.GB).gb()));
        assertThat(ByteSizeUnit.TB.toTB(10), is(new ByteSizeValue(10, ByteSizeUnit.TB).tb()));
        assertThat(ByteSizeUnit.PB.toPB(10), is(new ByteSizeValue(10, ByteSizeUnit.PB).pb()));
    }

    @Test
    public void testToString() {
        assertThat("10b", is(new ByteSizeValue(10, ByteSizeUnit.BYTES).toString()));
        assertThat("1.5kb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.BYTES).toString()));
        assertThat("1.5mb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.KB).toString()));
        assertThat("1.5gb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.MB).toString()));
        assertThat("1.5tb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.GB).toString()));
        assertThat("1.5pb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.TB).toString()));
        assertThat("1536pb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.PB).toString()));
    }

    @Test
    public void testParsing() {
        assertThat(ByteSizeValue.parseBytesSizeValue("42pb").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42P").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42PB").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54tb").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54T").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54TB").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12gb").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12G").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12GB").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12M").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1b").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23kb").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23k").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23").toString(), is("23b"));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testFailOnEmptyParsing() {
        assertThat(ByteSizeValue.parseBytesSizeValue("").toString(), is("23kb"));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testFailOnEmptyNumberParsing() {
        assertThat(ByteSizeValue.parseBytesSizeValue("g").toString(), is("23b"));
    }
}