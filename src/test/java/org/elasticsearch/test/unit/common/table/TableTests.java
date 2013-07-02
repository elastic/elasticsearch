/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.common.table;

import org.elasticsearch.common.table.Row;
import org.testng.annotations.Test;
import org.elasticsearch.common.table.Table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TableTests {
    @Test
    public void testTable() {
        Table tab = new Table();
        tab.addRow(new Row().addCell("123").addCell("4567"));
        tab.addRow(new Row().addCell("1234").addCell("567890123"));
        assertThat(tab.render(), equalTo("123  4567\n1234 567890123"));
    }

    @Test
    public void testHeader() {
        Table tab = new Table();
        tab.addRow(new Row().addCell("loooong").addCell("short"), true);
        tab.addRow(new Row().addCell("012").addCell("3456789"));
        assertThat(tab.render(true), equalTo("loooong short\n012     3456789"));
        assertThat(tab.render(), equalTo("012     3456789"));
    }
}
