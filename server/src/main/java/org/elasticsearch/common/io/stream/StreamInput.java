/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A stream from this node to another node. Technically, it can also be streamed to a byte array but that is mostly for testing.
 *
 * This class's methods are optimized so you can put the methods that read and write a class next to each other and you can scan them
 * visually for differences. That means that most variables should be read and written in a single line so even large objects fit both
 * reading and writing on the screen. It also means that the methods on this class are named very similarly to {@link StreamOutput}. Finally
 * it means that the "barrier to entry" for adding new methods to this class is relatively low even though it is a shared class with code
 * everywhere. That being said, this class deals primarily with {@code List}s rather than Arrays. For the most part calls should adapt to
 * lists, either by storing {@code List}s internally or just converting to and from a {@code List} when calling. This comment is repeated
 * on {@link StreamInput}.
 */
public abstract class StreamInput extends InputStream {

    private TransportVersion version = TransportVersion.current();

    /**
     * The transport version the data is serialized as.
     */
    public TransportVersion getTransportVersion() {
        return this.version;
    }

    /**
     * Set the transport version of the data in this stream.
     */
    public void setTransportVersion(TransportVersion version) {
        this.version = version;
    }

    /**
     * Reads and returns a single byte.
     */
    public abstract byte readByte() throws IOException;

    /**
     * Reads a specified number of bytes into an array at the specified offset.
     *
     * @param b      the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len    the number of bytes to read
     */
    public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

    // force implementing bulk reads to avoid accidentally slow implementations
    @Override
    public abstract int read(byte[] b, int off, int len) throws IOException;

    /**
     * Reads a bytes reference from this stream, copying any bytes read to a new {@code byte[]}. Use {@link #readReleasableBytesReference()}
     * when reading large bytes references where possible top avoid needless allocations and copying.
     */
    public BytesReference readBytesReference() throws IOException {
        int length = readArraySize();
        return readBytesReference(length);
    }

    /**
     * Reads a releasable bytes reference from this stream. Unlike {@link #readBytesReference()} the returned bytes reference may reference
     * bytes in a pooled buffer and must be explicitly released via {@link ReleasableBytesReference#close()} once no longer used.
     * Prefer this method over {@link #readBytesReference()} when reading large bytes references to avoid allocations and copying.
     */
    public ReleasableBytesReference readReleasableBytesReference() throws IOException {
        return ReleasableBytesReference.wrap(readBytesReference());
    }

    /**
     * Same as {@link #readBytesReference()} but with an explicitly provided length.
     * @param length number of bytes to read
     */
    public ReleasableBytesReference readReleasableBytesReference(int length) throws IOException {
        return ReleasableBytesReference.wrap(readBytesReference(length));
    }

    /**
     * Reads the same bytes returned by {@link #readReleasableBytesReference()} but does not retain a reference to these bytes.
     * The returned {@link BytesReference} thus only contains valid content as long as the underlying buffer has not been released.
     * This method should be preferred over {@link #readReleasableBytesReference()} when the returned reference is known to not be used
     * past the lifetime of the underlying buffer as it requires fewer allocations and does not require a potentially costly reference
     * count change.
     */
    public BytesReference readSlicedBytesReference() throws IOException {
        return readBytesReference();
    }

    public BytesReference readSlicedBytesReference(int bytes) throws IOException {
        return readBytesReference(bytes);
    }

    /**
     * Checks if this {@link InputStream} supports {@link #readAllToReleasableBytesReference()}.
     */
    public boolean supportReadAllToReleasableBytesReference() {
        return false;
    }

    /**
     * Reads all remaining bytes in the stream as a releasable bytes reference.
     * Similarly to {@link #readReleasableBytesReference} the returned bytes reference may reference bytes in a
     * pooled buffer and must be explicitly released via {@link ReleasableBytesReference#close()} once no longer used.
     * However, unlike {@link #readReleasableBytesReference()}, this method doesn't have the prefix size.
     * <p>
     * NOTE: Always check {@link #supportReadAllToReleasableBytesReference()} before calling this method.
     */
    public ReleasableBytesReference readAllToReleasableBytesReference() throws IOException {
        assert false : "This InputStream doesn't support readAllToReleasableBytesReference";
        throw new UnsupportedOperationException("This InputStream doesn't support readAllToReleasableBytesReference");
    }

    /**
     * Reads an optional bytes reference from this stream. It might hold an actual reference to the underlying bytes of the stream. Use this
     * only if you must differentiate null from empty. Use {@link StreamInput#readBytesReference()} and
     * {@link StreamOutput#writeBytesReference(BytesReference)} if you do not.
     */
    @Nullable
    public BytesReference readOptionalBytesReference() throws IOException {
        int length = readVInt() - 1;
        if (length < 0) {
            return null;
        }
        return readBytesReference(length);
    }

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference(int length) throws IOException {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new BytesArray(bytes, 0, length);
    }

    public BytesRef readBytesRef() throws IOException {
        int length = readArraySize();
        return readBytesRef(length);
    }

    public BytesRef readBytesRef(int length) throws IOException {
        if (length == 0) {
            return new BytesRef();
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new BytesRef(bytes, 0, length);
    }

    public void readFully(byte[] b) throws IOException {
        readBytes(b, 0, b.length);
    }

    public short readShort() throws IOException {
        return (short) (((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
    }

    /**
     * Reads four bytes and returns an int.
     */
    public int readInt() throws IOException {
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16) | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
    }

    /**
     * Reads an optional {@link Integer}.
     */
    public Integer readOptionalInt() throws IOException {
        if (readBoolean()) {
            return readInt();
        }
        return null;
    }

    /**
     * Reads an int stored in variable-length format.  Reads between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #readInt}
     */
    public int readVInt() throws IOException {
        return readVIntSlow();
    }

    protected final int readVIntSlow() throws IOException {
        byte b = readByte();
        int i = b & 0x7F;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        if ((b & 0x80) != 0) {
            throwOnBrokenVInt(b, i);
        }
        return i | ((b & 0x7F) << 28);
    }

    protected static void throwOnBrokenVInt(byte b, int accumulated) throws IOException {
        throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(accumulated));
    }

    /**
     * Reads eight bytes and returns a long.
     */
    public long readLong() throws IOException {
        return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a long stored in variable-length format. Reads between one and ten bytes. Smaller values take fewer bytes. Negative numbers
     * are encoded in ten bytes so prefer {@link #readLong()} or {@link #readZLong()} for negative numbers.
     */
    public long readVLong() throws IOException {
        return readVLongSlow();
    }

    protected final long readVLongSlow() throws IOException {
        byte b = readByte();
        long i = b & 0x7FL;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= ((b & 0x7FL) << 56);
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        if (b != 0 && b != 1) {
            throwOnBrokenVLong(b, i);
        }
        i |= ((long) b) << 63;
        return i;
    }

    protected static void throwOnBrokenVLong(byte b, long accumulated) throws IOException {
        throw new IOException("Invalid vlong (" + Integer.toHexString(b) + " << 63) | " + Long.toHexString(accumulated));
    }

    @Nullable
    public Long readOptionalVLong() throws IOException {
        if (readBoolean()) {
            return readVLong();
        }
        return null;
    }

    public long readZLong() throws IOException {
        long accumulator = 0L;
        int i = 0;
        long currentByte;
        while (((currentByte = readByte()) & 0x80L) != 0) {
            accumulator |= (currentByte & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IOException("variable-length stream is too long");
            }
        }
        return BitUtil.zigZagDecode(accumulator | (currentByte << i));
    }

    @Nullable
    public Long readOptionalLong() throws IOException {
        if (readBoolean()) {
            return readLong();
        }
        return null;
    }

    public BigInteger readBigInteger() throws IOException {
        return new BigInteger(readString());
    }

    private Text readText(int length) throws IOException {
        byte[] bytes = new byte[length];
        if (length > 0) {
            readBytes(bytes, 0, length);
        }
        var encoded = new XContentString.UTF8Bytes(bytes);
        return new Text(encoded);
    }

    @Nullable
    public Text readOptionalText() throws IOException {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        return readText(length);
    }

    public Text readText() throws IOException {
        // use Text so we can cache the string if it's ever converted to it
        int length = readInt();
        return readText(length);
    }

    @Nullable
    public String readOptionalString() throws IOException {
        if (readBoolean()) {
            return readString();
        }
        return null;
    }

    @Nullable
    public SecureString readOptionalSecureString() throws IOException {
        SecureString value = null;
        BytesReference bytesRef = readOptionalBytesReference();
        if (bytesRef != null) {
            byte[] bytes = BytesReference.toBytes(bytesRef);
            try {
                value = new SecureString(CharArrays.utf8BytesToChars(bytes));
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        return value;
    }

    @Nullable
    public Float readOptionalFloat() throws IOException {
        if (readBoolean()) {
            return readFloat();
        }
        return null;
    }

    @Nullable
    public Integer readOptionalVInt() throws IOException {
        if (readBoolean()) {
            return readVInt();
        }
        return null;
    }

    // Maximum char-count to de-serialize via the thread-local CharsRef buffer
    private static final int SMALL_STRING_LIMIT = 1024;

    // Reusable bytes for deserializing strings
    private static final ThreadLocal<byte[]> stringReadBuffer = ThreadLocal.withInitial(() -> new byte[1024]);

    // Thread-local buffer for smaller strings
    private static final ThreadLocal<char[]> smallSpare = ThreadLocal.withInitial(() -> new char[SMALL_STRING_LIMIT]);

    // Larger buffer used for long strings that can't fit into the thread-local buffer
    // We don't use a CharsRefBuilder since we exactly know the size of the character array up front
    // this prevents calling grow for every character since we don't need this
    private char[] largeSpare;

    private char[] ensureLargeSpare(int charCount) {
        char[] spare = largeSpare;
        if (spare == null || spare.length < charCount) {
            // we don't use ArrayUtils.grow since there is no need to copy the array
            spare = new char[ArrayUtil.oversize(charCount, Character.BYTES)];
            largeSpare = spare;
        }
        return spare;
    }

    public String readString() throws IOException {
        final int charCount = readArraySize();
        return doReadString(charCount);
    }

    protected String doReadString(final int charCount) throws IOException {
        final char[] charBuffer = charCount > SMALL_STRING_LIMIT ? ensureLargeSpare(charCount) : smallSpare.get();

        int charsOffset = 0;
        int offsetByteArray = 0;
        int sizeByteArray = 0;
        int missingFromPartial = 0;
        final byte[] byteBuffer = stringReadBuffer.get();
        for (; charsOffset < charCount;) {
            final int charsLeft = charCount - charsOffset;
            int bufferFree = byteBuffer.length - sizeByteArray;
            // Determine the minimum amount of bytes that are left in the string
            final int minRemainingBytes;
            if (missingFromPartial > 0) {
                // One byte for each remaining char except for the already partially read char
                minRemainingBytes = missingFromPartial + charsLeft - 1;
                missingFromPartial = 0;
            } else {
                // Each char has at least a single byte
                minRemainingBytes = charsLeft;
            }
            final int toRead;
            if (bufferFree < minRemainingBytes) {
                // We don't have enough space left in the byte array to read as much as we'd like to so we free up as many bytes in the
                // buffer by moving unused bytes that didn't make up a full char in the last iteration to the beginning of the buffer,
                // if there are any
                if (offsetByteArray > 0) {
                    sizeByteArray = sizeByteArray - offsetByteArray;
                    switch (sizeByteArray) { // We only have 0, 1 or 2 => no need to bother with a native call to System#arrayCopy
                        case 1 -> byteBuffer[0] = byteBuffer[offsetByteArray];
                        case 2 -> {
                            byteBuffer[0] = byteBuffer[offsetByteArray];
                            byteBuffer[1] = byteBuffer[offsetByteArray + 1];
                        }
                    }
                    assert sizeByteArray <= 2 : "We never copy more than 2 bytes here since a char is 3 bytes max";
                    toRead = Math.min(bufferFree + offsetByteArray, minRemainingBytes);
                    offsetByteArray = 0;
                } else {
                    toRead = bufferFree;
                }
            } else {
                toRead = minRemainingBytes;
            }
            readBytes(byteBuffer, sizeByteArray, toRead);
            sizeByteArray += toRead;
            // As long as we at least have three bytes buffered we don't need to do any bounds checking when getting the next char since we
            // read 3 bytes per char/iteration at most
            for (; offsetByteArray < sizeByteArray - 2; offsetByteArray++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
                switch (c >> 4) {
                    case 0, 1, 2, 3, 4, 5, 6, 7 -> charBuffer[charsOffset++] = (char) c;
                    case 12, 13 -> charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[++offsetByteArray] & 0x3F);
                    case 14 -> charBuffer[charsOffset++] = (char) ((c & 0x0F) << 12 | (byteBuffer[++offsetByteArray] & 0x3F) << 6
                        | (byteBuffer[++offsetByteArray] & 0x3F));
                    default -> throwOnBrokenChar(c);
                }
            }
            // try to extract chars from remaining bytes with bounds checks for multi-byte chars
            final int bufferedBytesRemaining = sizeByteArray - offsetByteArray;
            for (int i = 0; i < bufferedBytesRemaining; i++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
                switch (c >> 4) {
                    case 0, 1, 2, 3, 4, 5, 6, 7 -> {
                        charBuffer[charsOffset++] = (char) c;
                        offsetByteArray++;
                    }
                    case 12, 13 -> {
                        missingFromPartial = 2 - (bufferedBytesRemaining - i);
                        if (missingFromPartial == 0) {
                            offsetByteArray++;
                            charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[offsetByteArray++] & 0x3F);
                        }
                        ++i;
                    }
                    case 14 -> {
                        missingFromPartial = 3 - (bufferedBytesRemaining - i);
                        ++i;
                    }
                    default -> throwOnBrokenChar(c);
                }
            }
        }
        return new String(charBuffer, 0, charCount);
    }

    protected String tryReadStringFromBytes(final byte[] bytes, final int start, final int limit, final int chars) throws IOException {
        final int end = start + chars;
        if (limit < end) {
            return null; // not enough bytes to read chars
        }
        for (int pos = start; pos < end; pos++) {
            if ((bytes[pos] & 0x80) != 0) {
                // not an ASCII char, fall back to reading a UTF-8 string
                return tryReadUtf8StringFromBytes(bytes, start, limit, pos, end - pos);
            }
        }
        skip(chars); // skip the number of chars (equals bytes) on the stream input
        // We already validated the top bit is never set (so there's no negatives).
        // Using ISO_8859_1 over US_ASCII safes another scan to check just that and is equivalent otherwise.
        return new String(bytes, start, chars, ISO_8859_1);
    }

    private String tryReadUtf8StringFromBytes(final byte[] bytes, final int start, final int limit, int pos, int chars) throws IOException {
        while (pos < limit && chars-- > 0) {
            int c = bytes[pos] & 0xff;
            switch (c >> 4) {
                case 0, 1, 2, 3, 4, 5, 6, 7 -> pos++;
                case 12, 13 -> pos += 2;
                case 14 -> {
                    // surrogate pairs are incorrectly encoded, these can't be directly read from bytes
                    if (maybeHighSurrogate(bytes, pos, limit)) return null;
                    pos += 3;
                }
                default -> throwOnBrokenChar(c);
            }
        }

        if (chars == 0 && pos <= limit) {
            pos = pos - start;
            skip(pos); // skip the number of bytes relative to start on the stream input
            return new String(bytes, start, pos, UTF_8);
        }

        // not enough bytes to read all chars from array
        return null;
    }

    private static boolean maybeHighSurrogate(final byte[] bytes, final int pos, final int limit) {
        if (pos + 2 >= limit) {
            return true; // beyond limit, we can't tell
        }
        int c1 = bytes[pos] & 0xff;
        int c2 = bytes[pos + 1] & 0xff;
        int c3 = bytes[pos + 2] & 0xff;
        int surrogateCandidate = ((c1 & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
        // check if in the high surrogate range
        return surrogateCandidate >= 0xD800 && surrogateCandidate <= 0xDBFF;
    }

    private static void throwOnBrokenChar(int c) throws IOException {
        throw new IOException("Invalid string; unexpected character: " + c + " hex: " + Integer.toHexString(c));
    }

    public SecureString readSecureString() throws IOException {
        BytesReference bytesRef = readSlicedBytesReference();
        final char[] chars;
        if (bytesRef.hasArray()) {
            chars = CharArrays.utf8BytesToChars(bytesRef.array(), bytesRef.arrayOffset(), bytesRef.length());
        } else {
            chars = CharArrays.utf8BytesToChars(BytesReference.toBytes(bytesRef));
        }
        return new SecureString(chars);
    }

    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Nullable
    public final Double readOptionalDouble() throws IOException {
        if (readBoolean()) {
            return readDouble();
        }
        return null;
    }

    /**
     * Reads a boolean.
     */
    public final boolean readBoolean() throws IOException {
        return readBoolean(readByte());
    }

    private static boolean readBoolean(final byte value) {
        if (value == 0) {
            return false;
        } else if (value == 1) {
            return true;
        } else {
            final String message = String.format(Locale.ROOT, "unexpected byte [0x%02x]", value);
            throw new IllegalStateException(message);
        }
    }

    @Nullable
    public final Boolean readOptionalBoolean() throws IOException {
        final byte value = readByte();
        if (value == 2) {
            return null;
        } else {
            return readBoolean(value);
        }
    }

    /**
     * Closes the stream to further operations.
     */
    @Override
    public abstract void close() throws IOException;

    @Override
    public abstract int available() throws IOException;

    public String[] readStringArray() throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = readString();
        }
        return ret;
    }

    @Nullable
    public String[] readOptionalStringArray() throws IOException {
        if (readBoolean()) {
            return readStringArray();
        }
        return null;
    }

    /**
     * Reads an optional byte array. It's effectively the same as readByteArray, except
     * it supports null.
     * @return a byte array or null
     * @throws IOException
     */
    @Nullable
    public byte[] readOptionalByteArray() throws IOException {
        if (readBoolean()) {
            return readByteArray();
        }
        return null;
    }

    /**
     * Reads an optional float array. It's effectively the same as readFloatArray, except
     * it supports null.
     * @return a float array or null
     * @throws IOException
     */
    @Nullable
    public float[] readOptionalFloatArray() throws IOException {
        if (readBoolean()) {
            return readFloatArray();
        }
        return null;
    }

    /**
     * Same as {@link #readMap(Writeable.Reader, Writeable.Reader)} but always reading string keys.
     */
    public <V> Map<String, V> readMap(Writeable.Reader<V> valueReader) throws IOException {
        return readMap(StreamInput::readString, valueReader, Maps::newHashMapWithExpectedSize);
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <K, V> Map<K, V> readMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader) throws IOException {
        return readMap(keyReader, valueReader, Maps::newHashMapWithExpectedSize);
    }

    public <K, V> Map<K, V> readOrderedMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader) throws IOException {
        return readMap(keyReader, valueReader, Maps::newLinkedHashMapWithExpectedSize);
    }

    private <K, V> Map<K, V> readMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader, IntFunction<Map<K, V>> constructor)
        throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<K, V> map = constructor.apply(size);
        for (int i = 0; i < size; i++) {
            K key = keyReader.read(this);
            V value = valueReader.read(this);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Read a {@link Map} of string keys to {@code V}-type {@link List}s.
     * <pre><code>
     * Map&lt;String, List&lt;String&gt;&gt; map = in.readMapOfLists(StreamInput::readString);
     * </code></pre>
     * If the map or a list in it contains any elements it will be mutable, otherwise either the empty map or empty lists it contains
     * might be immutable.
     *
     * @param valueReader The value reader
     * @return Never {@code null}.
     */
    public <V> Map<String, List<V>> readMapOfLists(final Writeable.Reader<V> valueReader) throws IOException {
        return readMap(i -> i.readCollectionAsList(valueReader));
    }

    /**
     * Reads a multiple {@code V}-values and then converts them to a {@code Map} using keyMapper.
     *
     * @param valueReader The value reader
     * @param keyMapper function to create a key from a value
     * @return Never {@code null}.
     */
    public <K, V> Map<K, V> readMapValues(final Writeable.Reader<V> valueReader, final Function<V, K> keyMapper) throws IOException {
        final int size = readArraySize();
        if (size == 0) {
            return Map.of();
        }
        final Map<K, V> map = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            V value = valueReader.read(this);
            map.put(keyMapper.apply(value), value);
        }
        return map;
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public Map<String, Object> readGenericMap() throws IOException {
        return (Map<String, Object>) readGenericValue();
    }

    /**
     * Same as {@link #readMap(Writeable.Reader, Writeable.Reader)} but always reading string keys.
     */
    public <V> Map<String, V> readImmutableMap(Writeable.Reader<V> valueReader) throws IOException {
        return readImmutableMap(StreamInput::readString, valueReader);
    }

    /**
     * Read a {@link Map} using the given key and value readers. The return Map is immutable.
     *
     * @param keyReader Method to read a key. Must not return null.
     * @param valueReader Method to read a value. Must not return null.
     * @return The immutable map
     */
    public <K, V> Map<K, V> readImmutableMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader) throws IOException {
        final int size = readVInt();
        if (size == 0) {
            return Map.of();
        } else if (size == 1) {
            return Map.of(keyReader.read(this), valueReader.read(this));
        }
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map.Entry<K, V> entries[] = new Map.Entry[size];
        for (int i = 0; i < size; ++i) {
            entries[i] = Map.entry(keyReader.read(this), valueReader.read(this));
        }
        return Map.ofEntries(entries);
    }

    /**
     * Read {@link ImmutableOpenMap} using given key and value readers.
     *
     * @param keyReader   key reader
     * @param valueReader value reader
     */
    public <K, V> ImmutableOpenMap<K, V> readImmutableOpenMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader)
        throws IOException {
        final int size = readVInt();
        if (size == 0) {
            return ImmutableOpenMap.of();
        }
        final ImmutableOpenMap.Builder<K, V> builder = ImmutableOpenMap.builder(size);
        for (int i = 0; i < size; i++) {
            builder.put(keyReader.read(this), valueReader.read(this));
        }
        return builder.build();
    }

    /**
     * Reads a value of unspecified type. If a collection is read then the collection will be mutable if it contains any entry but might
     * be immutable if it is empty.
     */
    @Nullable
    public Object readGenericValue() throws IOException {
        byte type = readByte();
        return switch (type) {
            case -1 -> null;
            case 0 -> readString();
            case 1 -> readInt();
            case 2 -> readLong();
            case 3 -> readFloat();
            case 4 -> readDouble();
            case 5 -> readBoolean();
            case 6 -> readByteArray();
            case 7 -> readCollection(StreamInput::readGenericValue, ArrayList::new, Collections.emptyList());
            case 8 -> readArray();
            case 9 -> getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)
                ? readOrderedMap(StreamInput::readGenericValue, StreamInput::readGenericValue)
                : readOrderedMap(StreamInput::readString, StreamInput::readGenericValue);
            case 10 -> getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)
                ? readMap(StreamInput::readGenericValue, StreamInput::readGenericValue)
                : readMap(StreamInput::readGenericValue);
            case 11 -> readByte();
            case 12 -> readDate();
            case 13 ->
                // this used to be DateTime from Joda, and then JodaCompatibleZonedDateTime
                // stream-wise it is the exact same as ZonedDateTime, a timezone id and long milliseconds
                readZonedDateTime();
            case 14 -> readBytesReference();
            case 15 -> readText();
            case 16 -> readShort();
            case 17 -> readIntArray();
            case 18 -> readLongArray();
            case 19 -> readFloatArray();
            case 20 -> readDoubleArray();
            case 21 -> readBytesRef();
            case 22 -> readGeoPoint();
            case 23 -> readZonedDateTime();
            case 24 -> readCollection(StreamInput::readGenericValue, Sets::newLinkedHashSetWithExpectedSize, Collections.emptySet());
            case 25 -> readCollection(StreamInput::readGenericValue, Sets::newHashSetWithExpectedSize, Collections.emptySet());
            case 26 -> readBigInteger();
            case 27 -> readOffsetTime();
            case 28 -> readDuration();
            case 29 -> readPeriod();
            case 30 -> readNamedWriteable(GenericNamedWriteable.class);
            default -> throw new IOException("Can't read unknown type [" + type + "]");
        };
    }

    /**
     * Read an {@link Instant} from the stream with nanosecond resolution
     */
    public final Instant readInstant() throws IOException {
        return Instant.ofEpochSecond(readLong(), readInt());
    }

    /**
     * Read an optional {@link Instant} from the stream. Returns <code>null</code> when
     * no instant is present.
     */
    @Nullable
    public final Instant readOptionalInstant() throws IOException {
        final boolean present = readBoolean();
        return present ? readInstant() : null;
    }

    private ZonedDateTime readZonedDateTime() throws IOException {
        final String timeZoneId = readString();
        final Instant instant;
        if (getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            instant = Instant.ofEpochSecond(readZLong(), readInt());
        } else {
            instant = Instant.ofEpochMilli(readLong());
        }
        return ZonedDateTime.ofInstant(instant, ZoneId.of(timeZoneId));
    }

    private OffsetTime readOffsetTime() throws IOException {
        final String zoneOffsetId = readString();
        return OffsetTime.of(LocalTime.ofNanoOfDay(readLong()), ZoneOffset.of(zoneOffsetId));
    }

    private Duration readDuration() throws IOException {
        final long seconds = readLong();
        final long nanos = readLong();
        return Duration.ofSeconds(seconds, nanos);
    }

    private Period readPeriod() throws IOException {
        final int years = readInt();
        final int months = readInt();
        final int days = readInt();
        return Period.of(years, months, days);
    }

    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private Object[] readArray() throws IOException {
        int size8 = readArraySize();
        if (size8 == 0) {
            return EMPTY_OBJECT_ARRAY;
        }
        Object[] list8 = new Object[size8];
        for (int i = 0; i < size8; i++) {
            list8[i] = readGenericValue();
        }
        return list8;
    }

    private Date readDate() throws IOException {
        return new Date(readLong());
    }

    /**
     * Reads a {@link GeoPoint} from this stream input
     */
    public GeoPoint readGeoPoint() throws IOException {
        return new GeoPoint(readDouble(), readDouble());
    }

    /**
     * Read a {@linkplain ZoneId}.
     */
    public ZoneId readZoneId() throws IOException {
        return ZoneId.of(readString());
    }

    /**
     * Read an optional {@linkplain ZoneId}.
     */
    public ZoneId readOptionalZoneId() throws IOException {
        if (readBoolean()) {
            return ZoneId.of(readString());
        }
        return null;
    }

    private static final int[] EMPTY_INT_ARRAY = new int[0];

    public int[] readIntArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_INT_ARRAY;
        }
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = readInt();
        }
        return values;
    }

    public int[] readVIntArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_INT_ARRAY;
        }
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = readVInt();
        }
        return values;
    }

    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    public long[] readLongArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = readLong();
        }
        return values;
    }

    public long[] readVLongArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = readVLong();
        }
        return values;
    }

    private static final float[] EMPTY_FLOAT_ARRAY = new float[0];

    public float[] readFloatArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_FLOAT_ARRAY;
        }
        float[] values = new float[length];
        for (int i = 0; i < length; i++) {
            values[i] = readFloat();
        }
        return values;
    }

    private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

    public double[] readDoubleArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_DOUBLE_ARRAY;
        }
        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = readDouble();
        }
        return values;
    }

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public byte[] readByteArray() throws IOException {
        final int length = readArraySize();
        if (length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        final byte[] bytes = new byte[length];
        readBytes(bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Reads an array from the stream using the specified {@link org.elasticsearch.common.io.stream.Writeable.Reader} to read array elements
     * from the stream. This method can be seen as the reader version of {@link StreamOutput#writeArray(Writeable.Writer, Object[])}. It is
     * assumed that the stream first contains a variable-length integer representing the size of the array, and then contains that many
     * elements that can be read from the stream.
     *
     * @param reader        the reader used to read individual elements
     * @param arraySupplier a supplier used to construct a new array
     * @param <T>           the type of the elements of the array
     * @return an array read from the stream
     * @throws IOException if an I/O exception occurs while reading the array
     */
    public <T> T[] readArray(final Writeable.Reader<T> reader, final IntFunction<T[]> arraySupplier) throws IOException {
        final int length = readArraySize();
        final T[] values = arraySupplier.apply(length);
        for (int i = 0; i < length; i++) {
            values[i] = reader.read(this);
        }
        return values;
    }

    public <T> T[] readOptionalArray(Writeable.Reader<T> reader, IntFunction<T[]> arraySupplier) throws IOException {
        return readBoolean() ? readArray(reader, arraySupplier) : null;
    }

    /**
     * Reads a possibly-null value using the given {@link org.elasticsearch.common.io.stream.Writeable.Reader}.
     *
     * @see StreamOutput#writeOptionalWriteable
     */
    // just an alias for readOptional() since we don't actually care whether T extends Writeable
    @Nullable
    public <T extends Writeable> T readOptionalWriteable(Writeable.Reader<T> reader) throws IOException {
        return readOptional(reader);
    }

    /**
     * Reads a possibly-null value using the given {@link org.elasticsearch.common.io.stream.Writeable.Reader}.
     *
     * @see StreamOutput#writeOptional
     */
    public <T> T readOptional(Writeable.Reader<T> reader) throws IOException {
        if (readBoolean()) {
            T t = reader.read(this);
            if (t == null) {
                throwOnNullRead(reader);
            }
            return t;
        } else {
            return null;
        }
    }

    protected static void throwOnNullRead(Writeable.Reader<?> reader) throws IOException {
        final IOException e = new IOException("Writeable.Reader [" + reader + "] returned null which is not allowed.");
        assert false : e;
        throw e;
    }

    @Nullable
    public <T extends Exception> T readException() throws IOException {
        return ElasticsearchException.readException(this);
    }

    /**
     * Get the registry of named writeables if this stream has one,
     * {@code null} otherwise.
     */
    public NamedWriteableRegistry namedWriteableRegistry() {
        return null;
    }

    /**
     * Reads a {@link NamedWriteable} from the current stream, by first reading its name and then looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     * Use {@link FilterInputStream} instead which wraps a stream and supports a {@link NamedWriteableRegistry} too.
     */
    @Nullable
    public <C extends NamedWriteable> C readNamedWriteable(@SuppressWarnings("unused") Class<C> categoryClass) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads a {@link NamedWriteable} from the current stream with the given name. It is assumed that the caller obtained the name
     * from other source, so it's not read from the stream. The name is used for looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     * Use {@link FilterInputStream} instead which wraps a stream and supports a {@link NamedWriteableRegistry} too.
     *
     * Prefer {@link StreamInput#readNamedWriteable(Class)} and {@link StreamOutput#writeNamedWriteable(NamedWriteable)} unless you
     * have a compelling reason to use this method instead.
     */
    @Nullable
    public <C extends NamedWriteable> C readNamedWriteable(
        @SuppressWarnings("unused") Class<C> categoryClass,
        @SuppressWarnings("unused") String name
    ) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads an optional {@link NamedWriteable}.
     */
    @Nullable
    public <C extends NamedWriteable> C readOptionalNamedWriteable(Class<C> categoryClass) throws IOException {
        if (readBoolean()) {
            return readNamedWriteable(categoryClass);
        }
        return null;
    }

    /**
     * Reads a list of objects which was written using {@link StreamOutput#writeCollection}. If the returned list contains any entries it
     * will be a (mutable) {@link ArrayList}. If it is empty it might be immutable.
     */
    public <T> List<T> readCollectionAsList(final Writeable.Reader<T> reader) throws IOException {
        return readCollection(reader, ArrayList::new, Collections.emptyList());
    }

    /**
     * Reads a list of objects which was written using {@link StreamOutput#writeCollection}. The returned list is immutable.
     */
    public <T> List<T> readCollectionAsImmutableList(final Writeable.Reader<T> reader) throws IOException {
        int count = readArraySize();
        // special cases small arrays, just like in java.util.List.of(...)
        return switch (count) {
            case 0 -> List.of();
            case 1 -> List.of(reader.read(this));
            case 2 -> List.of(reader.read(this), reader.read(this));
            default -> {
                Object[] entries = new Object[count];
                for (int i = 0; i < count; i++) {
                    entries[i] = reader.read(this);
                }
                @SuppressWarnings("unchecked")
                T[] typedEntries = (T[]) entries;
                yield List.of(typedEntries);
            }
        };
    }

    /**
     * Reads a list of strings which was written using {@link StreamOutput#writeStringCollection}. The returned list is immutable.
     */
    public List<String> readStringCollectionAsImmutableList() throws IOException {
        return readCollectionAsImmutableList(StreamInput::readString);
    }

    /**
     * Reads a list of strings which was written using {@link StreamOutput#writeStringCollection}. If the returned list contains any entries
     * it will be a (mutable) {@link ArrayList}. If it is empty it might be immutable.
     */
    public List<String> readStringCollectionAsList() throws IOException {
        return readCollectionAsList(StreamInput::readString);
    }

    /**
     * Reads a possibly-{@code null} list which was written using {@link StreamOutput#writeOptionalCollection}. If the returned list
     * contains any entries it will be a (mutable) {@link ArrayList}. If it is empty it might be immutable.
     */
    @Nullable
    public <T> List<T> readOptionalCollectionAsList(final Writeable.Reader<T> reader) throws IOException {
        final boolean isPresent = readBoolean();
        return isPresent ? readCollectionAsList(reader) : null;
    }

    /**
     * Reads a possibly-{@code null} list of strings which was written using {@link StreamOutput#writeOptionalStringCollection}. If the
     * returned list contains any entries it will be a (mutable) {@link ArrayList}. If it is empty it might be immutable.
     */
    @Nullable
    public List<String> readOptionalStringCollectionAsList() throws IOException {
        return readOptionalCollectionAsList(StreamInput::readString);
    }

    /**
     * Reads a set of objects which was written using {@link StreamOutput#writeCollection}. If the returned set contains any entries it
     * will a (mutable) {@link HashSet}. If it is empty it might be immutable. The collection that was originally written should also have
     * been a set.
     */
    public <T> Set<T> readCollectionAsSet(Writeable.Reader<T> reader) throws IOException {
        return readCollection(reader, Sets::newHashSetWithExpectedSize, Collections.emptySet());
    }

    /**
     * Reads a set of objects which was written using {@link StreamOutput#writeCollection}}. The returned set is immutable. The collection
     * that was originally written should also have been a set.
     */
    public <T> Set<T> readCollectionAsImmutableSet(final Writeable.Reader<T> reader) throws IOException {
        int count = readArraySize();
        // special cases small arrays, just like in java.util.Set.of(...)
        return switch (count) {
            case 0 -> Set.of();
            case 1 -> Set.of(reader.read(this));
            case 2 -> Set.of(reader.read(this), reader.read(this));
            default -> {
                Object[] entries = new Object[count];
                for (int i = 0; i < count; i++) {
                    entries[i] = reader.read(this);
                }
                @SuppressWarnings("unchecked")
                T[] typedEntries = (T[]) entries;
                yield Set.of(typedEntries);
            }
        };
    }

    /**
     * Reads a list of {@link NamedWriteable}s which was written using {@link StreamOutput#writeNamedWriteableCollection}. If the returned
     * list contains any entries it will be a (mutable) {@link ArrayList}. If it is empty it might be immutable.
     */
    public <T extends NamedWriteable> List<T> readNamedWriteableCollectionAsList(Class<T> categoryClass) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads a collection which was written using {@link StreamOutput#writeCollection}, accumulating the results using the provided
     * consumer.
     */
    public <C> C readCollection(IntFunction<C> constructor, CheckedBiConsumer<StreamInput, C, IOException> itemConsumer)
        throws IOException {
        int count = readArraySize();
        var result = constructor.apply(count);
        for (int i = 0; i < count; i++) {
            itemConsumer.accept(this, result);
        }
        return result;
    }

    /**
     * Reads a collection, comprising a call to {@link #readVInt} for the size, followed by that many invocations of {@code reader}.
     *
     * @param reader      reads each object in the collection
     * @param constructor constructs the collection of the given (positive) size
     * @param empty       constructs an empty collection
     */
    private <T, C extends Collection<? super T>> C readCollection(Writeable.Reader<T> reader, IntFunction<C> constructor, C empty)
        throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return empty;
        }
        C builder = constructor.apply(count);
        for (int i = 0; i < count; i++) {
            builder.add(reader.read(this));
        }
        assert builder.size() == count
            : Strings.format("read %d items but resulting collection has size %d - were duplicates removed?", count, builder.size());
        return builder;
    }

    /**
     * Reads an enum with type {@code E} that was serialized based on the value of its ordinal. Enums serialized like this must have a
     * corresponding test which uses {@code EnumSerializationTestUtils#assertEnumSerialization} to fix the wire protocol.
     */
    public <E extends Enum<E>> E readEnum(Class<E> enumClass) throws IOException {
        return readEnum(enumClass, enumClass.getEnumConstants());
    }

    /**
     * Reads an optional enum with type {@code E} that was serialized based on the value of its ordinal. Enums serialized like this must
     * have a corresponding test which uses {@code EnumSerializationTestUtils#assertEnumSerialization} to fix the wire protocol.
     */
    @Nullable
    public <E extends Enum<E>> E readOptionalEnum(Class<E> enumClass) throws IOException {
        if (readBoolean()) {
            return readEnum(enumClass, enumClass.getEnumConstants());
        } else {
            return null;
        }
    }

    private <E extends Enum<E>> E readEnum(Class<E> enumClass, E[] values) throws IOException {
        int ordinal = readVInt();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IOException("Unknown " + enumClass.getSimpleName() + " ordinal [" + ordinal + "]");
        }
        return values[ordinal];
    }

    /**
     * Reads a set of enums with type {@code E} that were serialized based on the value of their ordinals. Enums serialized like this must
     * have a corresponding test which uses {@code EnumSerializationTestUtils#assertEnumSerialization} to fix the wire protocol.
     */
    public <E extends Enum<E>> EnumSet<E> readEnumSet(Class<E> enumClass) throws IOException {
        int size = readVInt();
        final EnumSet<E> res = EnumSet.noneOf(enumClass);
        if (size == 0) {
            return res;
        }
        final E[] values = enumClass.getEnumConstants();
        for (int i = 0; i < size; i++) {
            res.add(readEnum(enumClass, values));
        }
        return res;
    }

    public static StreamInput wrap(byte[] bytes) {
        return wrap(bytes, 0, bytes.length);
    }

    public static StreamInput wrap(byte[] bytes, int offset, int length) {
        return new ByteBufferStreamInput(ByteBuffer.wrap(bytes, offset, length));
    }

    /**
     * Reads a vint via {@link #readVInt()} and applies basic checks to ensure the read array size is sane.
     * This method uses {@link #ensureCanReadBytes(int)} to ensure this stream has enough bytes to read for the read array size.
     */
    protected int readArraySize() throws IOException {
        final int arraySize = readVInt();
        if (arraySize > ArrayUtil.MAX_ARRAY_LENGTH) {
            throwExceedsMaxArraySize(arraySize);
        }
        if (arraySize < 0) {
            throwNegative(arraySize);
        }
        // let's do a sanity check that if we are reading an array size that is bigger that the remaining bytes we can safely
        // throw an exception instead of allocating the array based on the size. A simple corrupted byte can make a node go OOM
        // if the size is large and for perf reasons we allocate arrays ahead of time
        ensureCanReadBytes(arraySize);
        return arraySize;
    }

    private static void throwNegative(int arraySize) {
        throw new NegativeArraySizeException("array size must be positive but was: " + arraySize);
    }

    private static void throwExceedsMaxArraySize(int arraySize) {
        throw new IllegalStateException("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH + " but was: " + arraySize);
    }

    /**
     * This method throws an {@link EOFException} if the given number of bytes can not be read from the stream. This method might
     * be a no-op depending on the underlying implementation if the information of the remaining bytes is not present.
     */
    protected abstract void ensureCanReadBytes(int length) throws EOFException;

    protected static void throwEOF(int bytesToRead, int bytesAvailable) throws EOFException {
        throw new EOFException("tried to read: " + bytesToRead + " bytes but only " + bytesAvailable + " remaining");
    }

    private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

    static {
        // assert the exact form of the TimeUnit values to ensure we're not silently broken by a JDK change
        if (Arrays.equals(
            TIME_UNITS,
            new TimeUnit[] {
                TimeUnit.NANOSECONDS,
                TimeUnit.MICROSECONDS,
                TimeUnit.MILLISECONDS,
                TimeUnit.SECONDS,
                TimeUnit.MINUTES,
                TimeUnit.HOURS,
                TimeUnit.DAYS }
        ) == false) {
            throw new AssertionError("Incompatible JDK version used that breaks assumptions on the structure of the TimeUnit enum");
        }
    }

    /**
     * Read a {@link TimeValue} from the stream
     */
    public TimeValue readTimeValue() throws IOException {
        final long duration = readZLong();
        final TimeUnit timeUnit = TIME_UNITS[readByte()];
        return switch (timeUnit) {
            // avoid unnecessary allocation for some common cases:
            case MILLISECONDS -> TimeValue.timeValueMillis(duration);
            case SECONDS -> TimeValue.timeValueSeconds(duration);
            case MINUTES -> TimeValue.timeValueMinutes(duration);
            default -> new TimeValue(duration, timeUnit);
        };
    }

    /**
     * Read an optional {@link TimeValue} from the stream, returning null if no TimeValue was written.
     */
    public @Nullable TimeValue readOptionalTimeValue() throws IOException {
        if (readBoolean()) {
            return readTimeValue();
        } else {
            return null;
        }
    }
}
