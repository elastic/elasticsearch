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

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;

public final class Uid {

    public static final char DELIMITER = '#';
    public static final byte DELIMITER_BYTE = 0x23;

    private final String type;

    private final String id;

    public Uid(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Uid uid = (Uid) o;

        if (id != null ? !id.equals(uid.id) : uid.id != null) return false;
        if (type != null ? !type.equals(uid.type) : uid.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return createUid(type, id);
    }

    public BytesRef toBytesRef() {
        return createUidAsBytes(type, id);
    }

    public static Uid createUid(String uid) {
        int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
        return new Uid(uid.substring(0, delimiterIndex), uid.substring(delimiterIndex + 1));
    }

    public static BytesRef createUidAsBytes(String type, String id) {
        return createUidAsBytes(new BytesRef(type), new BytesRef(id));
    }

    public static BytesRef createUidAsBytes(String type, BytesRef id) {
        return createUidAsBytes(new BytesRef(type), id);
    }

    public static BytesRef createUidAsBytes(BytesRef type, BytesRef id) {
        final BytesRef ref = new BytesRef(type.length + 1 + id.length);
        System.arraycopy(type.bytes, type.offset, ref.bytes, 0, type.length);
        ref.offset = type.length;
        ref.bytes[ref.offset++] = DELIMITER_BYTE;
        System.arraycopy(id.bytes, id.offset, ref.bytes, ref.offset, id.length);
        ref.offset = 0;
        ref.length = ref.bytes.length;
        return ref;
    }

    public static BytesRef[] createUidsForTypesAndId(Collection<String> types, Object id) {
        return createUidsForTypesAndIds(types, Collections.singletonList(id));
    }

    public static BytesRef[] createUidsForTypesAndIds(Collection<String> types, Collection<?> ids) {
        BytesRef[] uids = new BytesRef[types.size() * ids.size()];
        BytesRefBuilder typeBytes = new BytesRefBuilder();
        BytesRefBuilder idBytes = new BytesRefBuilder();
        int index = 0;
        for (String type : types) {
            typeBytes.copyChars(type);
            for (Object id : ids) {
                uids[index++] = Uid.createUidAsBytes(typeBytes.get(), BytesRefs.toBytesRef(id, idBytes));
            }
        }
        return uids;
    }

    public static String createUid(String type, String id) {
        return type + DELIMITER + id;
    }

    private static final int UTF8 = 0xff;
    private static final int NUMERIC = 0xfe;
    private static final int BASE64_ESCAPE = 0xfd;

    static boolean isURLBase64WithoutPadding(String id) {
        // We are not lenient about padding chars ('=') otherwise
        // 'xxx=' and 'xxx' could be considered the same id
        final int length = id.length();
        switch (length & 0x03) {
            case 0:
                break;
            case 1:
                return false;
            case 2:
                // the last 2 symbols (12 bits) are encoding 1 byte (8 bits)
                // so the last symbol only actually uses 8-6=2 bits and can only take 4 values
                char last = id.charAt(length - 1);
                if (last != 'A' && last != 'Q' && last != 'g' && last != 'w') {
                    return false;
                }
                break;
            case 3:
                // The last 3 symbols (18 bits) are encoding 2 bytes (16 bits)
                // so the last symbol only actually uses 16-12=4 bits and can only take 16 values
                last = id.charAt(length - 1);
                if (last != 'A' && last != 'E' && last != 'I' && last != 'M' && last != 'Q'&& last != 'U'&& last != 'Y'
                    && last != 'c'&& last != 'g'&& last != 'k' && last != 'o' && last != 's' && last != 'w'
                    && last != '0' && last != '4' && last != '8') {
                    return false;
                }
                break;
            default:
                // number & 0x03 is always in [0,3]
                throw new AssertionError("Impossible case");
        }
        for (int i = 0; i < length; ++i) {
            final char c = id.charAt(i);
            final boolean allowed =
                (c >= '0' && c <= '9') ||
                    (c >= 'A' && c <= 'Z') ||
                    (c >= 'a' && c <= 'z') ||
                    c == '-' || c == '_';
            if (allowed == false) {
                return false;
            }
        }
        return true;
    }

    static boolean isPositiveNumeric(String id) {
        for (int i = 0; i < id.length(); ++i) {
            final char c = id.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /** With numeric ids, we just fold two consecutive chars in a single byte
     *  and use 0x0f as an end marker. */
    private static BytesRef encodeNumericId(String id) {
        byte[] b = new byte[1 + (id.length() + 1) / 2];
        b[0] = (byte) NUMERIC;
        for (int i = 0; i < id.length(); i += 2) {
            int b1 = id.charAt(i) - '0';
            int b2;
            if (i + 1 == id.length()) {
                b2 = 0x0f; // end marker
            } else {
                b2 = id.charAt(i + 1) - '0';
            }
            b[1 + i/2] = (byte) ((b1 << 4) | b2);
        }
        return new BytesRef(b);
    }

    /** With base64 ids, we decode and prepend an escape char in the cases that
     *  it could be mixed up with numeric or utf8 encoding. In the majority of
     *  cases (253/256) the encoded id is exactly the binary form. */
    private static BytesRef encodeBase64Id(String id) {
        byte[] b = Base64.getUrlDecoder().decode(id);
        if (Byte.toUnsignedInt(b[0]) >= BASE64_ESCAPE) {
            byte[] newB = new byte[b.length + 1];
            newB[0] = (byte) BASE64_ESCAPE;
            System.arraycopy(b, 0, newB, 1, b.length);
            b = newB;
        }
        return new BytesRef(b, 0, b.length);
    }

    private static BytesRef encodeUtf8Id(String id) {
        byte[] b = new byte[1 + UnicodeUtil.maxUTF8Length(id.length())];
        // Prepend a byte that indicates that the content is an utf8 string
        b[0] = (byte) UTF8;
        int length = UnicodeUtil.UTF16toUTF8(id, 0, id.length(), b, 1);
        return new BytesRef(b, 0, length);
    }

    /** Encode an id for storage in the index. This encoding is optimized for
     *  numeric and base64 ids, which are encoded in a much denser way than
     *  what UTF8 would do.
     *  @see #decodeId */
    public static BytesRef encodeId(String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Ids can't be empty");
        }
        if (isPositiveNumeric(id)) {
            // common for ids that come from databases with auto-increments
            return encodeNumericId(id);
        } else if (isURLBase64WithoutPadding(id)) {
            // common since it applies to autogenerated ids
            return encodeBase64Id(id);
        } else {
            return encodeUtf8Id(id);
        }
    }

    private static String decodeNumericId(byte[] idBytes, int offset, int len) {
        assert Byte.toUnsignedInt(idBytes[offset]) == NUMERIC;
        int length = (len - 1) * 2;
        char[] chars = new char[length];
        for (int i = 1; i < len; ++i) {
            final int b = Byte.toUnsignedInt(idBytes[offset + i]);
            final int b1 = (b >>> 4);
            final int b2 = b & 0x0f;
            chars[(i - 1) * 2] = (char) (b1 + '0');
            if (i == len - 1 && b2 == 0x0f) {
                length--;
                break;
            }
            chars[(i - 1) * 2 + 1] = (char) (b2 + '0');
        }
        return new String(chars, 0, length);
    }

    private static String decodeUtf8Id(byte[] idBytes, int offset, int length) {
        assert Byte.toUnsignedInt(idBytes[offset]) == UTF8;
        return new BytesRef(idBytes, offset + 1, length - 1).utf8ToString();
    }

    private static String decodeBase64Id(byte[] idBytes, int offset, int length) {
        assert Byte.toUnsignedInt(idBytes[offset]) <= BASE64_ESCAPE;
        if (Byte.toUnsignedInt(idBytes[offset]) == BASE64_ESCAPE) {
            idBytes = Arrays.copyOfRange(idBytes, offset + 1, offset + length);
        } else if ((idBytes.length == length && offset == 0) == false) { // no need to copy if it's not a slice
            idBytes = Arrays.copyOfRange(idBytes, offset, offset + length);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(idBytes);
    }

    /** Decode an indexed id back to its original form.
     *  @see #encodeId */
    public static String decodeId(byte[] idBytes) {
        return decodeId(idBytes, 0, idBytes.length);
    }

    /** Decode an indexed id back to its original form.
     *  @see #encodeId */
    public static String decodeId(byte[] idBytes, int offset, int length) {
        if (length == 0) {
            throw new IllegalArgumentException("Ids can't be empty");
        }
        final int magicChar = Byte.toUnsignedInt(idBytes[offset]);
        switch (magicChar) {
            case NUMERIC:
                return decodeNumericId(idBytes, offset, length);
            case UTF8:
                return decodeUtf8Id(idBytes, offset, length);
            default:
                return decodeBase64Id(idBytes, offset, length);
        }
    }
}
