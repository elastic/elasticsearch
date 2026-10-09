/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.Base64;

/**
 * The identity of a document: its user-visible {@code id}, an optional {@code slice} and the {@code term} indexed into
 * {@code _id} that uniqueness, versioning, GET and delete resolve against.
 * <p>
 * Without slicing the term is simply {@link #encodeId(String) encodeId(id)} and {@link #slice()} is {@code null}, so
 * {@code id} and the uid are one and the same. With slicing the term is the compound {@code encodeId(id + '#' + slice)},
 * which keeps the same id unique per slice. Callers hold a {@link Uid} rather than juggling a raw {@code id} String and a
 * {@code term} {@link BytesRef} separately.
 */
public final class Uid {

    public static final byte DELIMITER_BYTE = 0x23;
    /** Separates the id from the slice in a compound term. Not a valid slice character, so it splits unambiguously. */
    private static final char DELIMITER = (char) DELIMITER_BYTE;

    private static final int UTF8 = 0xff;
    private static final int NUMERIC = 0xfe;
    /** Escape byte prepended to base64-decoded IDs when the first byte is >= 0xfd */
    public static final int BASE64_ESCAPE = 0xfd;

    private final String id;
    @Nullable
    private final String slice;
    private final BytesRef term;

    private Uid(String id, @Nullable String slice, BytesRef term) {
        this.id = id;
        this.slice = slice;
        this.term = term;
    }

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
                if (last != 'A'
                    && last != 'E'
                    && last != 'I'
                    && last != 'M'
                    && last != 'Q'
                    && last != 'U'
                    && last != 'Y'
                    && last != 'c'
                    && last != 'g'
                    && last != 'k'
                    && last != 'o'
                    && last != 's'
                    && last != 'w'
                    && last != '0'
                    && last != '4'
                    && last != '8') {
                    return false;
                }
                break;
            default:
                // number & 0x03 is always in [0,3]
                throw new AssertionError("Impossible case");
        }
        for (int i = 0; i < length; ++i) {
            final char c = id.charAt(i);
            final boolean allowed = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '-' || c == '_';
            if (allowed == false) {
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
            b[1 + i / 2] = (byte) ((b1 << 4) | b2);
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
        byte[] b = new byte[1 + UnicodeUtil.calcUTF16toUTF8Length(id, 0, id.length())];
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
        if (Numbers.isPositiveNumeric(id)) {
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
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(idBytes);
    }

    /** Decode an indexed id back to its original form.
     *  @see #encodeId */
    public static String decodeId(BytesRef idBytes) {
        return decodeId(idBytes.bytes, idBytes.offset, idBytes.length);
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
        return switch (magicChar) {
            case NUMERIC -> decodeNumericId(idBytes, offset, length);
            case UTF8 -> decodeUtf8Id(idBytes, offset, length);
            default -> decodeBase64Id(idBytes, offset, length);
        };
    }

    /** Encode the compound {@code id + '#' + slice} term used as the uid of a slice-scoped document. */
    public static BytesRef encodeCompoundId(String id, String slice) {
        if (slice.isEmpty()) {
            // An empty slice would encode to encodeId(id + "#"), which is the slice-free search term - they must not collide.
            throw new IllegalArgumentException("slice must not be empty for compound _id encoding");
        }
        return encodeId(id + DELIMITER + slice);
    }

    /** The uid of a plain (non-sliced) document: its term is {@link #encodeId(String)} and its slice is {@code null}. */
    public static Uid of(String id) {
        return new Uid(id, null, encodeId(id));
    }

    /** The uid of a slice-scoped document: its term is the compound {@code id#slice} encoding. */
    public static Uid of(String id, String slice) {
        return new Uid(id, slice, encodeCompoundId(id, slice));
    }

    /**
     * Build the uid for a document, compound when {@code sliceEnabled}. A slice-enabled index requires the slice, which
     * arrives as the routing value; a plain index must not carry one.
     */
    public static Uid create(boolean sliceEnabled, String id, @Nullable String slice) {
        if (sliceEnabled) {
            if (slice == null) {
                throw new IllegalArgumentException("unable to create _id as slice is enabled but slice is null");
            }
            return of(id, slice);
        }
        return of(id);
    }

    /** Reconstruct the uid from its indexed/stored term, splitting off the slice when {@code sliceEnabled}. */
    public static Uid fromTerm(BytesRef term, boolean sliceEnabled) {
        if (sliceEnabled) {
            String compound = decodeId(term.bytes, term.offset, term.length);
            int i = compound.lastIndexOf(DELIMITER);
            return new Uid(compound.substring(0, i), compound.substring(i + 1), term);
        }
        return new Uid(decodeId(term), null, term);
    }

    /** The user-visible id, with any slice stripped. */
    public String id() {
        return id;
    }

    /** The slice this uid is scoped to, or {@code null} when the index is not slice-enabled. */
    @Nullable
    public String slice() {
        return slice;
    }

    /** The term indexed into {@code _id} that uniqueness, versioning, GET and delete resolve against. */
    public BytesRef term() {
        return term;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Uid other && term.equals(other.term);
    }

    @Override
    public int hashCode() {
        return term.hashCode();
    }

    @Override
    public String toString() {
        return slice == null ? id : id + DELIMITER + slice;
    }
}
