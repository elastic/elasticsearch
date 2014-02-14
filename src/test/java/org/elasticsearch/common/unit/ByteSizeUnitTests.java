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

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.unit.ByteSizeUnit.*;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ByteSizeUnitTests extends ElasticsearchTestCase {

    @Test
    public void testBytes() {
        assertThat(BYTES.toBytes(1), equalTo(1l));
        assertThat(BYTES.toKB(1024), equalTo(1l));
        assertThat(BYTES.toMB(1024 * 1024), equalTo(1l));
        assertThat(BYTES.toGB(1024 * 1024 * 1024), equalTo(1l));
    }

    @Test
    public void testKB() {
        assertThat(KB.toBytes(1), equalTo(1024l));
        assertThat(KB.toKB(1), equalTo(1l));
        assertThat(KB.toMB(1024), equalTo(1l));
        assertThat(KB.toGB(1024 * 1024), equalTo(1l));
    }

    @Test
    public void testMB() {
        assertThat(MB.toBytes(1), equalTo(1024l * 1024));
        assertThat(MB.toKB(1), equalTo(1024l));
        assertThat(MB.toMB(1), equalTo(1l));
        assertThat(MB.toGB(1024), equalTo(1l));
    }

    @Test
    public void testGB() {
        assertThat(GB.toBytes(1), equalTo(1024l * 1024 * 1024));
        assertThat(GB.toKB(1), equalTo(1024l * 1024));
        assertThat(GB.toMB(1), equalTo(1024l));
        assertThat(GB.toGB(1), equalTo(1l));
    }

    @Test
    public void testTB() {
        assertThat(TB.toBytes(1), equalTo(1024l * 1024 * 1024 * 1024));
        assertThat(TB.toKB(1), equalTo(1024l * 1024 * 1024));
        assertThat(TB.toMB(1), equalTo(1024l * 1024));
        assertThat(TB.toGB(1), equalTo(1024l));
        assertThat(TB.toTB(1), equalTo(1l));
    }

    @Test
    public void testPB() {
        assertThat(PB.toBytes(1), equalTo(1024l * 1024 * 1024 * 1024 * 1024));
        assertThat(PB.toKB(1), equalTo(1024l * 1024 * 1024 * 1024));
        assertThat(PB.toMB(1), equalTo(1024l * 1024 * 1024));
        assertThat(PB.toGB(1), equalTo(1024l * 1024));
        assertThat(PB.toTB(1), equalTo(1024l));
        assertThat(PB.toPB(1), equalTo(1l));
    }
}
