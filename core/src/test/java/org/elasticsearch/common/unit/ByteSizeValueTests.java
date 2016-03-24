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
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ByteSizeValueTests extends ESTestCase {
    public void testActualPeta() {
        MatcherAssert.assertThat(new ByteSizeValue(4, ByteSizeUnit.PB).bytes(), equalTo(4503599627370496L));
    }

    public void testActualTera() {
        MatcherAssert.assertThat(new ByteSizeValue(4, ByteSizeUnit.TB).bytes(), equalTo(4398046511104L));
    }

    public void testActual() {
        MatcherAssert.assertThat(new ByteSizeValue(4, ByteSizeUnit.GB).bytes(), equalTo(4294967296L));
    }

    public void testSimple() {
        assertThat(ByteSizeUnit.BYTES.toBytes(10), is(new ByteSizeValue(10, ByteSizeUnit.BYTES).bytes()));
        assertThat(ByteSizeUnit.KB.toKB(10), is(new ByteSizeValue(10, ByteSizeUnit.KB).kb()));
        assertThat(ByteSizeUnit.MB.toMB(10), is(new ByteSizeValue(10, ByteSizeUnit.MB).mb()));
        assertThat(ByteSizeUnit.GB.toGB(10), is(new ByteSizeValue(10, ByteSizeUnit.GB).gb()));
        assertThat(ByteSizeUnit.TB.toTB(10), is(new ByteSizeValue(10, ByteSizeUnit.TB).tb()));
        assertThat(ByteSizeUnit.PB.toPB(10), is(new ByteSizeValue(10, ByteSizeUnit.PB).pb()));
    }

    public void testEquality() {
        String[] equalValues = new String[]{"1GB", "1024MB", "1048576KB", "1073741824B"};
        ByteSizeValue value1 = ByteSizeValue.parseBytesSizeValue(randomFrom(equalValues), "equalTest");
        ByteSizeValue value2 = ByteSizeValue.parseBytesSizeValue(randomFrom(equalValues), "equalTest");
        assertThat(value1, equalTo(value2));
    }

    public void testToString() {
        assertThat("10b", is(new ByteSizeValue(10, ByteSizeUnit.BYTES).toString()));
        assertThat("1.5kb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.BYTES).toString()));
        assertThat("1.5mb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.KB).toString()));
        assertThat("1.5gb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.MB).toString()));
        assertThat("1.5tb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.GB).toString()));
        assertThat("1.5pb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.TB).toString()));
        assertThat("1536pb", is(new ByteSizeValue((long) (1024 * 1.5), ByteSizeUnit.PB).toString()));
    }

    public void testParsing() {
        assertThat(ByteSizeValue.parseBytesSizeValue("42PB", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 PB", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42pb", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 pb", "testParsing").toString(), is("42pb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("42P", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 P", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42p", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 p", "testParsing").toString(), is("42pb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("54TB", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 TB", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54tb", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 tb", "testParsing").toString(), is("54tb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("54T", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 T", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54t", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 t", "testParsing").toString(), is("54tb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("12GB", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 GB", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12gb", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 gb", "testParsing").toString(), is("12gb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("12G", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 G", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12g", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 g", "testParsing").toString(), is("12gb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("12M", "testParsing").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 M", "testParsing").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12m", "testParsing").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 m", "testParsing").toString(), is("12mb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("23KB", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 KB", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23kb", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 kb", "testParsing").toString(), is("23kb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("23K", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 K", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23k", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 k", "testParsing").toString(), is("23kb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("1B", "testParsing").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1 B", "testParsing").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1b", "testParsing").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1 b", "testParsing").toString(), is("1b"));
    }

    public void testFailOnMissingUnits() {
        try {
            ByteSizeValue.parseBytesSizeValue("23", "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse setting [test]"));
        }
    }

    public void testFailOnUnknownUnits() {
        try {
            ByteSizeValue.parseBytesSizeValue("23jw", "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse setting [test]"));
        }
    }

    public void testFailOnEmptyParsing() {
        try {
            assertThat(ByteSizeValue.parseBytesSizeValue("", "emptyParsing").toString(), is("23kb"));
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse setting [emptyParsing]"));
        }
    }

    public void testFailOnEmptyNumberParsing() {
        try {
            assertThat(ByteSizeValue.parseBytesSizeValue("g", "emptyNumberParsing").toString(), is("23b"));
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse [g]"));
        }
    }

    public void testNoDotsAllowed() {
        try {
            ByteSizeValue.parseBytesSizeValue("42b.", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse setting [test]"));
        }
    }
}
