/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.joda.time.DateTimeZone;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.elasticsearch.ElasticsearchException.readStackTrace;

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

    private Version version = Version.CURRENT;

    /**
     * The version of the node on the other side of this stream.
     */
    public Version getVersion() {
        return this.version;
    }

    /**
     * Set the version of the node on the other side of this stream.
     */
    public void setVersion(Version version) {
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
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16)
                | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
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


    @Nullable
    public Text readOptionalText() throws IOException {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        return new Text(readBytesReference(length));
    }

    public Text readText() throws IOException {
        // use StringAndBytes so we can cache the string if its ever converted to it
        int length = readInt();
        return new Text(readBytesReference(length));
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
    private static final ThreadLocal<CharsRef> smallSpare = ThreadLocal.withInitial(() -> new CharsRef(SMALL_STRING_LIMIT));

    // Larger buffer used for long strings that can't fit into the thread-local buffer
    // We don't use a CharsRefBuilder since we exactly know the size of the character array up front
    // this prevents calling grow for every character since we don't need this
    private CharsRef largeSpare;

    public String readString() throws IOException {
        final int charCount = readArraySize();
        final CharsRef charsRef;
        if (charCount > SMALL_STRING_LIMIT) {
            if (largeSpare == null) {
                largeSpare = new CharsRef(ArrayUtil.oversize(charCount, Character.BYTES));
            } else if (largeSpare.chars.length < charCount) {
                // we don't use ArrayUtils.grow since there is no need to copy the array
                largeSpare.chars = new char[ArrayUtil.oversize(charCount, Character.BYTES)];
            }
            charsRef = largeSpare;
        } else {
            charsRef = smallSpare.get();
        }
        charsRef.length = charCount;
        int charsOffset = 0;
        int offsetByteArray = 0;
        int sizeByteArray = 0;
        int missingFromPartial = 0;
        final byte[] byteBuffer = stringReadBuffer.get();
        final char[] charBuffer = charsRef.chars;
        for (; charsOffset < charCount; ) {
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
                        case 1:
                            byteBuffer[0] = byteBuffer[offsetByteArray];
                            break;
                        case 2:
                            byteBuffer[0] = byteBuffer[offsetByteArray];
                            byteBuffer[1] = byteBuffer[offsetByteArray + 1];
                            break;
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
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        charBuffer[charsOffset++] = (char) c;
                        break;
                    case 12:
                    case 13:
                        charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[++offsetByteArray] & 0x3F);
                        break;
                    case 14:
                        charBuffer[charsOffset++] = (char) (
                            (c & 0x0F) << 12 | (byteBuffer[++offsetByteArray] & 0x3F) << 6 | (byteBuffer[++offsetByteArray] & 0x3F));
                        break;
                    default:
                        throwOnBrokenChar(c);
                }
            }
            // try to extract chars from remaining bytes with bounds checks for multi-byte chars
            final int bufferedBytesRemaining = sizeByteArray - offsetByteArray;
            for (int i = 0; i < bufferedBytesRemaining; i++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
                switch (c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        charBuffer[charsOffset++] = (char) c;
                        offsetByteArray++;
                        break;
                    case 12:
                    case 13:
                        missingFromPartial = 2 - (bufferedBytesRemaining - i);
                        if (missingFromPartial == 0) {
                            offsetByteArray++;
                            charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[offsetByteArray++] & 0x3F);
                        }
                        ++i;
                        break;
                    case 14:
                        missingFromPartial = 3 - (bufferedBytesRemaining - i);
                        ++i;
                        break;
                    default:
                        throwOnBrokenChar(c);
                }
            }
        }
        return charsRef.toString();
    }

    private static void throwOnBrokenChar(int c) throws IOException {
        throw new IOException("Invalid string; unexpected character: " + c + " hex: " + Integer.toHexString(c));
    }

    public SecureString readSecureString() throws IOException {
        BytesReference bytesRef = readBytesReference();
        byte[] bytes = BytesReference.toBytes(bytesRef);
        try {
            return new SecureString(CharArrays.utf8BytesToChars(bytes));
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
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

    private boolean readBoolean(final byte value) {
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
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <K, V> Map<K, V> readMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader) throws IOException {
        return readMap(keyReader, valueReader, HashMap::new);
    }

    public <K, V> Map<K, V> readOrderedMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader) throws IOException {
        return readMap(keyReader, valueReader, LinkedHashMap::new);
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
     * Read a {@link Map} of {@code K}-type keys to {@code V}-type {@link List}s.
     * <pre><code>
     * Map&lt;String, List&lt;String&gt;&gt; map = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
     * </code></pre>
     * If the map or a list in it contains any elements it will be mutable, otherwise either the empty map or empty lists it contains
     * might be immutable.
     *
     * @param keyReader The key reader
     * @param valueReader The value reader
     * @return Never {@code null}.
     */
    public <K, V> Map<K, List<V>> readMapOfLists(final Writeable.Reader<K> keyReader, final Writeable.Reader<V> valueReader)
            throws IOException {
        final int size = readArraySize();
        if (size == 0) {
            return Collections.emptyMap();
        }
        final Map<K, List<V>> map = new HashMap<>(size);
        for (int i = 0; i < size; ++i) {
            map.put(keyReader.read(this), readList(valueReader));
        }
        return map;
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public Map<String, Object> readMap() throws IOException {
        return (Map<String, Object>) readGenericValue();
    }

    /**
     * Read {@link ImmutableOpenMap} using given key and value readers.
     *
     * @param keyReader   key reader
     * @param valueReader value reader
     */
    public <K, V> ImmutableOpenMap<K, V> readImmutableMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader)
            throws IOException {
        final int size = readVInt();
        if (size == 0) {
            return ImmutableOpenMap.of();
        }
        final ImmutableOpenMap.Builder<K,V> builder = ImmutableOpenMap.builder(size);
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
        switch (type) {
            case -1:
                return null;
            case 0:
                return readString();
            case 1:
                return readInt();
            case 2:
                return readLong();
            case 3:
                return readFloat();
            case 4:
                return readDouble();
            case 5:
                return readBoolean();
            case 6:
                return readByteArray();
            case 7:
                return readArrayList();
            case 8:
                return readArray();
            case 9:
                return readLinkedHashMap();
            case 10:
                return readHashMap();
            case 11:
                return readByte();
            case 12:
                return readDate();
            case 13:
                return readDateTime();
            case 14:
                return readBytesReference();
            case 15:
                return readText();
            case 16:
                return readShort();
            case 17:
                return readIntArray();
            case 18:
                return readLongArray();
            case 19:
                return readFloatArray();
            case 20:
                return readDoubleArray();
            case 21:
                return readBytesRef();
            case 22:
                return readGeoPoint();
            case 23:
                return readZonedDateTime();
            case 24:
                return readCollection(StreamInput::readGenericValue, LinkedHashSet::new, Collections.emptySet());
            case 25:
                return readCollection(StreamInput::readGenericValue, HashSet::new, Collections.emptySet());
            case 26:
                return readBigInteger();
            case 27:
                return readOffsetTime();
            default:
                throw new IOException("Can't read unknown type [" + type + "]");
        }
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

    private List<Object> readArrayList() throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Collections.emptyList();
        }
        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(readGenericValue());
        }
        return list;
    }

    private JodaCompatibleZonedDateTime readDateTime() throws IOException {
        // we reuse DateTime to communicate with older nodes that don't know about the joda compat layer, but
        // here we are on a new node so we always want a compat datetime
        final ZoneId zoneId = DateUtils.dateTimeZoneToZoneId(DateTimeZone.forID(readString()));
        long millis = readLong();
        return new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), zoneId);
    }

    private ZonedDateTime readZonedDateTime() throws IOException {
        final String timeZoneId = readString();
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(readLong()), ZoneId.of(timeZoneId));
    }

    private OffsetTime readOffsetTime() throws IOException {
        final String zoneOffsetId = readString();
        return OffsetTime.of(LocalTime.ofNanoOfDay(readLong()), ZoneOffset.of(zoneOffsetId));
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

    private Map<String, Object> readLinkedHashMap() throws IOException {
        int size9 = readArraySize();
        if (size9 == 0) {
            return Collections.emptyMap();
        }
        Map<String, Object> map9 = new LinkedHashMap<>(size9);
        for (int i = 0; i < size9; i++) {
            map9.put(readString(), readGenericValue());
        }
        return map9;
    }

    private Map<String, Object> readHashMap() throws IOException {
        int size10 = readArraySize();
        if (size10 == 0) {
            return Collections.emptyMap();
        }
        Map<String, Object> map10 = new HashMap<>(size10);
        for (int i = 0; i < size10; i++) {
            map10.put(readString(), readGenericValue());
        }
        return map10;
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
     * Read a {@linkplain DateTimeZone}.
     */
    public DateTimeZone readTimeZone() throws IOException {
        return DateTimeZone.forID(readString());
    }

    /**
     * Read an optional {@linkplain DateTimeZone}.
     */
    public DateTimeZone readOptionalTimeZone() throws IOException {
        if (readBoolean()) {
            return DateTimeZone.forID(readString());
        }
        return null;
    }

    /**
     * Read a {@linkplain DateTimeZone}.
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

    @Nullable
    public <T extends Writeable> T readOptionalWriteable(Writeable.Reader<T> reader) throws IOException {
        if (readBoolean()) {
            T t = reader.read(this);
            if (t == null) {
                throw new IOException("Writeable.Reader [" + reader
                        + "] returned null which is not allowed and probably means it screwed up the stream.");
            }
            return t;
        } else {
            return null;
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <T extends Exception> T readException() throws IOException {
        if (readBoolean()) {
            int key = readVInt();
            switch (key) {
                case 0:
                    final int ord = readVInt();
                    return (T) ElasticsearchException.readException(this, ord);
                case 1:
                    String msg1 = readOptionalString();
                    String resource1 = readOptionalString();
                    return (T) readStackTrace(new CorruptIndexException(msg1, resource1, readException()), this);
                case 2:
                    String resource2 = readOptionalString();
                    int version2 = readInt();
                    int minVersion2 = readInt();
                    int maxVersion2 = readInt();
                    return (T) readStackTrace(new IndexFormatTooNewException(resource2, version2, minVersion2, maxVersion2), this);
                case 3:
                    String resource3 = readOptionalString();
                    if (readBoolean()) {
                        int version3 = readInt();
                        int minVersion3 = readInt();
                        int maxVersion3 = readInt();
                        return (T) readStackTrace(new IndexFormatTooOldException(resource3, version3, minVersion3, maxVersion3), this);
                    } else {
                        String version3 = readOptionalString();
                        return (T) readStackTrace(new IndexFormatTooOldException(resource3, version3), this);
                    }
                case 4:
                    return (T) readStackTrace(new NullPointerException(readOptionalString()), this);
                case 5:
                    return (T) readStackTrace(new NumberFormatException(readOptionalString()), this);
                case 6:
                    return (T) readStackTrace(new IllegalArgumentException(readOptionalString(), readException()), this);
                case 7:
                    return (T) readStackTrace(new AlreadyClosedException(readOptionalString(), readException()), this);
                case 8:
                    return (T) readStackTrace(new EOFException(readOptionalString()), this);
                case 9:
                    return (T) readStackTrace(new SecurityException(readOptionalString(), readException()), this);
                case 10:
                    return (T) readStackTrace(new StringIndexOutOfBoundsException(readOptionalString()), this);
                case 11:
                    return (T) readStackTrace(new ArrayIndexOutOfBoundsException(readOptionalString()), this);
                case 12:
                    return (T) readStackTrace(new FileNotFoundException(readOptionalString()), this);
                case 13:
                    final int subclass = readVInt();
                    final String file = readOptionalString();
                    final String other = readOptionalString();
                    final String reason = readOptionalString();
                    readOptionalString(); // skip the msg - it's composed from file, other and reason
                    final Exception exception;
                    switch (subclass) {
                        case 0:
                            exception = new NoSuchFileException(file, other, reason);
                            break;
                        case 1:
                            exception = new NotDirectoryException(file);
                            break;
                        case 2:
                            exception = new DirectoryNotEmptyException(file);
                            break;
                        case 3:
                            exception = new AtomicMoveNotSupportedException(file, other, reason);
                            break;
                        case 4:
                            exception = new FileAlreadyExistsException(file, other, reason);
                            break;
                        case 5:
                            exception = new AccessDeniedException(file, other, reason);
                            break;
                        case 6:
                            exception = new FileSystemLoopException(file);
                            break;
                        case 7:
                            exception = new FileSystemException(file, other, reason);
                            break;
                        default:
                            throw new IllegalStateException("unknown FileSystemException with index " + subclass);
                    }
                    return (T) readStackTrace(exception, this);
                case 14:
                    return (T) readStackTrace(new IllegalStateException(readOptionalString(), readException()), this);
                case 15:
                    return (T) readStackTrace(new LockObtainFailedException(readOptionalString(), readException()), this);
                case 16:
                    return (T) readStackTrace(new InterruptedException(readOptionalString()), this);
                case 17:
                    return (T) readStackTrace(new IOException(readOptionalString(), readException()), this);
                case 18:
                    final boolean isExecutorShutdown = readBoolean();
                    return (T) readStackTrace(new EsRejectedExecutionException(readOptionalString(), isExecutorShutdown), this);
                default:
                    throw new IOException("no such exception for id: " + key);
            }
        }
        return null;
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
    public <C extends NamedWriteable> C readNamedWriteable(@SuppressWarnings("unused") Class<C> categoryClass,
                                                           @SuppressWarnings("unused") String name) throws IOException {
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
     * Reads a list of objects. The list is expected to have been written using {@link StreamOutput#writeList(List)}.
     * If the returned list contains any entries it will be mutable. If it is empty it might be immutable.
     *
     * @return the list of objects
     * @throws IOException if an I/O exception occurs reading the list
     */
    public <T> List<T> readList(final Writeable.Reader<T> reader) throws IOException {
        return readCollection(reader, ArrayList::new, Collections.emptyList());
    }

    /**
     * Reads a list of strings. The list is expected to have been written using {@link StreamOutput#writeStringCollection(Collection)}.
     * If the returned list contains any entries it will be mutable. If it is empty it might be immutable.
     *
     * @return the list of strings
     * @throws IOException if an I/O exception occurs reading the list
     */
    public List<String> readStringList() throws IOException {
        return readList(StreamInput::readString);
    }

    /**
     * Reads an optional list of strings. The list is expected to have been written using
     * {@link StreamOutput#writeOptionalStringCollection(Collection)}. If the returned list contains any entries it will be mutable.
     * If it is empty it might be immutable.
     *
     * @return the list of strings
     * @throws IOException if an I/O exception occurs reading the list
     */
    public List<String> readOptionalStringList() throws IOException {
        final boolean isPresent = readBoolean();
        if (isPresent) {
            return readList(StreamInput::readString);
        } else {
            return null;
        }
    }

    /**
     * Reads a set of objects. If the returned set contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <T> Set<T> readSet(Writeable.Reader<T> reader) throws IOException {
        return readCollection(reader, HashSet::new, Collections.emptySet());
    }

    /**
     * Reads a collection of objects
     */
    private <T, C extends Collection<? super T>> C readCollection(Writeable.Reader<T> reader,
                                                                  IntFunction<C> constructor,
                                                                  C empty) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return empty;
        }
        C builder = constructor.apply(count);
        for (int i=0; i<count; i++) {
            builder.add(reader.read(this));
        }
        return builder;
    }

    /**
     * Reads a list of {@link NamedWriteable}s. If the returned list contains any entries it will be mutable.
     * If it is empty it might be immutable.
     */
    public <T extends NamedWriteable> List<T> readNamedWriteableList(Class<T> categoryClass) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return Collections.emptyList();
        }
        List<T> builder = new ArrayList<>(count);
        for (int i=0; i<count; i++) {
            builder.add(readNamedWriteable(categoryClass));
        }
        return builder;
    }

    /**
     * Reads an enum with type E that was serialized based on the value of its ordinal
     */
    public <E extends Enum<E>> E readEnum(Class<E> enumClass) throws IOException {
        return readEnum(enumClass, enumClass.getEnumConstants());
    }

    /**
     * Reads an optional enum with type E that was serialized based on the value of its ordinal
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
     * Reads an enum with type E that was serialized based on the value of it's ordinal
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
            throw new IllegalStateException("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH  + " but was: " + arraySize);
        }
        if (arraySize < 0) {
            throw new NegativeArraySizeException("array size must be positive but was: " + arraySize);
        }
        // lets do a sanity check that if we are reading an array size that is bigger that the remaining bytes we can safely
        // throw an exception instead of allocating the array based on the size. A simple corrutpted byte can make a node go OOM
        // if the size is large and for perf reasons we allocate arrays ahead of time
        ensureCanReadBytes(arraySize);
        return arraySize;
    }

    /**
     * This method throws an {@link EOFException} if the given number of bytes can not be read from the this stream. This method might
     * be a no-op depending on the underlying implementation if the information of the remaining bytes is not present.
     */
    protected abstract void ensureCanReadBytes(int length) throws EOFException;

    private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

    static {
        // assert the exact form of the TimeUnit values to ensure we're not silently broken by a JDK change
        if (Arrays.equals(TIME_UNITS, new TimeUnit[]{TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS}) == false) {
            throw new AssertionError("Incompatible JDK version used that breaks assumptions on the structure of the TimeUnit enum");
        }
    }

    /**
     * Read a {@link TimeValue} from the stream
     */
    public TimeValue readTimeValue() throws IOException {
        long duration = readZLong();
        TimeUnit timeUnit = TIME_UNITS[readByte()];
        return new TimeValue(duration, timeUnit);
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
