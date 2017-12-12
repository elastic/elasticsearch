/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfoTests.randomMetaColumnInfo;


public class MetaColumnResponseTests extends ESTestCase {
    static MetaColumnResponse randomMetaColumnResponse() {
        int size = between(0, 10);
        List<MetaColumnInfo> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            columns.add(randomMetaColumnInfo());
        }
        return new MetaColumnResponse(columns);
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(MetaColumnRequestTests::randomMetaColumnRequest, randomMetaColumnResponse());
    }

    public void testToString() {
        assertEquals("MetaColumnResponse<>", new MetaColumnResponse(emptyList()).toString());
        assertEquals("MetaColumnResponse<a.doc.col1<type=[VARCHAR] size=[100] position=[1]>, "
                + "a.doc.col2<type=[INTEGER] size=[16] position=[2]>, "
                + "b.doc.col1<type=[VARCHAR] size=[100] position=[1]>>", new MetaColumnResponse(Arrays.asList(
                new MetaColumnInfo("a.doc", "col1", JDBCType.VARCHAR, 100, 1),
                new MetaColumnInfo("a.doc", "col2", JDBCType.INTEGER, 16, 2),
                new MetaColumnInfo("b.doc", "col1", JDBCType.VARCHAR, 100, 1))).toString());
    }
}
