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
import java.util.Locale;

/**
 * An opaque cursor token that encodes the state needed to fetch the next page
 * of a paginated ES|QL query result. The token includes the {@link StorageType}
 * so that fetch and delete operations can route to the correct storage backend.
 */
public record EsqlCursor(StorageType storageType, String cursorId, int pageIndex) {

    /**
     * Identifies the storage backend used for a paginated cursor's data.
     */
    public enum StorageType {
        INDEX((byte) 0),
        DISK((byte) 1);

        private final byte wire;

        StorageType(byte wire) {
            this.wire = wire;
        }

        public byte wire() {
            return wire;
        }

        public static StorageType fromWire(byte b) {
            return switch (b) {
                case 0 -> INDEX;
                case 1 -> DISK;
                default -> throw new IllegalArgumentException("unknown cursor storage type byte [" + b + "]");
            };
        }

        public static StorageType fromString(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "index" -> INDEX;
                case "disk" -> DISK;
                default -> throw new IllegalArgumentException("unknown cursor_storage value [" + value + "], expected [index] or [disk]");
            };
        }
    }

    public String encode() {
        byte[] idBytes = cursorId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + idBytes.length + 4);
        buf.put(storageType.wire());
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putInt(pageIndex);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buf.array());
    }

    public static EsqlCursor decode(String token) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(token);
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            StorageType storageType = StorageType.fromWire(buf.get());
            int idLen = buf.getInt();
            byte[] idBytes = new byte[idLen];
            buf.get(idBytes);
            String cursorId = new String(idBytes, StandardCharsets.UTF_8);
            int pageIndex = buf.getInt();
            return new EsqlCursor(storageType, cursorId, pageIndex);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid cursor token", e);
        }
    }
}
