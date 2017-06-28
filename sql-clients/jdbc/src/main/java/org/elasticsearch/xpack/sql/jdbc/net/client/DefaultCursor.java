/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import java.sql.SQLException;
import java.util.List;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

class DefaultCursor implements Cursor {

    private final JdbcHttpClient client;
    private final RequestMeta meta;

    private final Page page;
    private int row = -1;
    private String requestId;

    DefaultCursor(JdbcHttpClient client, String scrollId, Page page, RequestMeta meta) {
        this.client = client;
        this.meta = meta;
        this.requestId = simplifyScrollId(scrollId);
        this.page = page;
    }

    private static String simplifyScrollId(String scrollId) {
        return StringUtils.hasText(scrollId) ? scrollId : null;
    }

    @Override
    public List<ColumnInfo> columns() {
        return page.columnInfo();
    }

    @Override
    public boolean next() throws SQLException {
        if (row < page.rows() - 1) {
            row++;
            return true;
        }
        else {
            if (requestId != null) {
                requestId = simplifyScrollId(client.nextPage(requestId, page, meta));
                row = -1;
                return next();
            }
            return false;
        }
    }

    @Override
    public Object column(int column) {
        return page.entry(row, column);
    }
}
