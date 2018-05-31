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
package org.elasticsearch.rest.action.cat;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

public class CatResponseTableTests extends ESTestCase {

    public void testFilterOneQualify() {
        CatResponseTable table = new CatResponseTable(new FakeRestRequest());
        String pattern = "some_pattern";
        table.startHeaders()
            .addCell("header1" )
            .addCell("header2")
            .endHeaders();
        table.setFilter(pattern);
        table.startRow()
            .addCell("foo")
            .addCell("bar")
            .endRow();
        table.startRow()
            .addCell("some_pattern")
            .addCell("bar")
            .endRow();
        assertEquals(1, table.getRows().size());
    }

    public void testFilterAllQualify() {
        CatResponseTable table = new CatResponseTable(new FakeRestRequest());
        String pattern = "some_pattern";
        table.startHeaders()
            .addCell("header1" )
            .addCell("header2")
            .endHeaders();
        table.setFilter(pattern);
        table.startRow()
            .addCell("foo")
            .addCell("some_pattern_1")
            .endRow();
        table.startRow()
            .addCell("some_pattern_2")
            .addCell("bar")
            .endRow();
        assertEquals(2, table.getRows().size());
    }

    public void testFilterNoneQualify() {
        CatResponseTable table = new CatResponseTable(new FakeRestRequest());
        String pattern = "some_other_pattern";
        table.startHeaders()
            .addCell("header1" )
            .addCell("header2")
            .endHeaders();
        table.setFilter(pattern);
        table.startRow()
            .addCell("foo")
            .addCell("bar")
            .endRow();
        table.startRow()
            .addCell("some_pattern")
            .addCell("bar")
            .endRow();
        assertEquals(0, table.getRows().size());
    }

    public void testInvertedTwoQualify() {
        CatResponseTable table = new CatResponseTable(new FakeRestRequest(), true);
        String pattern = "some_pattern";
        table.startHeaders()
            .addCell("header1" )
            .addCell("header2")
            .endHeaders();
        table.setFilter(pattern);
        table.startRow()
            .addCell("foo")
            .addCell("bar")
            .endRow();
        table.startRow()
            .addCell("some_pattern")
            .addCell("bar234")
            .endRow();
        table.startRow()
            .addCell("some_other_pattern")
            .addCell("bar123")
            .endRow();
        assertEquals(2, table.getRows().size());
    }

    public void testInvertedAllQualify() {
        CatResponseTable table = new CatResponseTable(new FakeRestRequest(), true);
        String pattern = "some_obscure_pattern";
        table.startHeaders()
            .addCell("header1" )
            .addCell("header2")
            .endHeaders();
        table.setFilter(pattern);
        table.startRow()
            .addCell("foo")
            .addCell("bar")
            .endRow();
        table.startRow()
            .addCell("some_pattern")
            .addCell("bar234")
            .endRow();
        assertEquals(2, table.getRows().size());
    }

    public void testNoFilter() {
        CatResponseTable table = new CatResponseTable(new FakeRestRequest());
        table.startHeaders()
            .addCell("header1" )
            .addCell("header2")
            .endHeaders();
        table.startRow()
            .addCell("foo")
            .addCell("bar")
            .endRow();
        table.startRow()
            .addCell("some_pattern")
            .addCell("bar")
            .endRow();
        assertEquals(2, table.getRows().size());
    }

}
