/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfoTests.doubleInfo;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfoTests.intInfo;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfoTests.randomValueFor;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfoTests.varcharInfo;
import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.assertRoundTrip;
import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.roundTrip;

public class PageTests extends ESTestCase {
    static Page randomPage() {
        int columns = between(0, 10);
        List<ColumnInfo> columnInfo = new ArrayList<>();
        for (int c = 0; c < columns; c++) {
            @SuppressWarnings("unchecked")
            Supplier<ColumnInfo> info = randomFrom(
                    () -> varcharInfo(randomAlphaOfLength(5)),
                    () -> intInfo(randomAlphaOfLength(5)),
                    () -> doubleInfo(randomAlphaOfLength(5))); 
            columnInfo.add(info.get());
        }
        return randomPageContents(columnInfo);
    }

    static Page randomPageContents(List<ColumnInfo> columnInfo) {
        Object[][] rows = new Object[between(0, 10)][]; 
        for (int r = 0; r < rows.length; r++) {
            rows[r] = new Object[columnInfo.size()];
            for (int c = 0; c < columnInfo.size(); c++) {
                rows[r][c] = randomValueFor(columnInfo.get(c));
            }
        }
        return new Page(columnInfo, rows);
    }

    public void testRoundTripNoReuse() throws IOException {
        Page example = randomPage();
        assertRoundTrip(example, Page::write, in -> {
            Page page = new Page(example.columnInfo());
            page.read(in);
            return page;
        });
    }

    public void testRoundTripReuse() throws IOException {
        Page example = randomPage();
        Page target = new Page(example.columnInfo());
        roundTrip(example, Page::write, in -> {target.read(in); return null;});
        assertEquals(example, target);

        example = randomPageContents(example.columnInfo());
        roundTrip(example, Page::write, in -> {target.read(in); return null;});
        assertEquals(example, target);
    }

    public void testToString() {
        assertEquals("\n\n",
                new Page(emptyList(), new Object[][] {
                        new Object[] {},
                        new Object[] {},
                }).toString());
        assertEquals("test\n",
                new Page(singletonList(varcharInfo("a")), new Object[][] {
                        new Object[] {"test"}
                }).toString());
        assertEquals("test, 1\n",
                new Page(Arrays.asList(varcharInfo("a"), intInfo("b")), new Object[][] {
                        new Object[] {"test", 1}
                }).toString());
        assertEquals("test, 1\nbar, 7\n",
                new Page(Arrays.asList(varcharInfo("a"), intInfo("b")), new Object[][] {
                        new Object[] {"test", 1},
                        new Object[] {"bar", 7}
                }).toString());
        
    }
}
