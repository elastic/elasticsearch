/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * An opaque cursor token that encodes the state needed to fetch the next page
 * of a paginated ES|QL query result. The {@code pageIndex} maps directly to
 * the page document suffix {@code _p{pageIndex}} in the cursor system index.
 */
public record EsqlCursor(String cursorId, int pageIndex, int pageSize) {

    public String encode() {
        byte[] idBytes = cursorId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length + 4 + 4);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putInt(pageIndex);
        buf.putInt(pageSize);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buf.array());
    }

    public static EsqlCursor decode(String token) {
        byte[] bytes = Base64.getUrlDecoder().decode(token);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int idLen = buf.getInt();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String cursorId = new String(idBytes, StandardCharsets.UTF_8);
        int pageIndex = buf.getInt();
        int pageSize = buf.getInt();
        return new EsqlCursor(cursorId, pageIndex, pageSize);
    }
}
