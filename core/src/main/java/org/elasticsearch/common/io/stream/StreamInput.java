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

package org.elasticsearch.common.io.stream;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.ElasticsearchException.readException;
import static org.elasticsearch.ElasticsearchException.readStackTrace;

public abstract class StreamInput extends InputStream {

    private final NamedWriteableRegistry namedWriteableRegistry;

    private Version version = Version.CURRENT;

    protected StreamInput() {
        this.namedWriteableRegistry = new NamedWriteableRegistry();
    }

    protected StreamInput(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public Version getVersion() {
        return this.version;
    }

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
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference() throws IOException {
        int length = readVInt();
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
        int length = readVInt();
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
     * Reads an int stored in variable-length format.  Reads between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #readInt}
     */
    public int readVInt() throws IOException {
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
        assert (b & 0x80) == 0;
        return i | ((b & 0x7F) << 28);
    }

    /**
     * Reads eight bytes and returns a long.
     */
    public long readLong() throws IOException {
        return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a long stored in variable-length format.  Reads between one and
     * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    public long readVLong() throws IOException {
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
        assert (b & 0x80) == 0;
        return i | ((b & 0x7FL) << 56);
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
    public Text readOptionalText() throws IOException {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        return new StringAndBytesText(readBytesReference(length));
    }

    public Text readText() throws IOException {
        // use StringAndBytes so we can cache the string if its ever converted to it
        int length = readInt();
        return new StringAndBytesText(readBytesReference(length));
    }

    @Nullable
    public String readOptionalString() throws IOException {
        if (readBoolean()) {
            return readString();
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

    private final CharsRefBuilder spare = new CharsRefBuilder();

    public String readString() throws IOException {
        final int charCount = readVInt();
        spare.clear();
        spare.grow(charCount);
        int c;
        while (spare.length() < charCount) {
            c = readByte() & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    spare.append((char) c);
                    break;
                case 12:
                case 13:
                    spare.append((char) ((c & 0x1F) << 6 | readByte() & 0x3F));
                    break;
                case 14:
                    spare.append((char) ((c & 0x0F) << 12 | (readByte() & 0x3F) << 6 | (readByte() & 0x3F) << 0));
                    break;
            }
        }
        return spare.toString();
    }


    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Reads a boolean.
     */
    public final boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Nullable
    public final Boolean readOptionalBoolean() throws IOException {
        byte val = readByte();
        if (val == 2) {
            return null;
        }
        if (val == 1) {
            return true;
        }
        return false;
    }

    /**
     * Resets the stream.
     */
    @Override
    public abstract void reset() throws IOException;

    /**
     * Closes the stream to further operations.
     */
    @Override
    public abstract void close() throws IOException;

    public String[] readStringArray() throws IOException {
        int size = readVInt();
        if (size == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = readString();
        }
        return ret;
    }

    public String[] readOptionalStringArray() throws IOException {
        if (readBoolean()) {
            return readStringArray();
        }
        return null;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public Map<String, Object> readMap() throws IOException {
        return (Map<String, Object>) readGenericValue();
    }

    @SuppressWarnings({"unchecked"})
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
                int bytesSize = readVInt();
                byte[] value = new byte[bytesSize];
                readBytes(value, 0, bytesSize);
                return value;
            case 7:
                int size = readVInt();
                List list = new ArrayList(size);
                for (int i = 0; i < size; i++) {
                    list.add(readGenericValue());
                }
                return list;
            case 8:
                int size8 = readVInt();
                Object[] list8 = new Object[size8];
                for (int i = 0; i < size8; i++) {
                    list8[i] = readGenericValue();
                }
                return list8;
            case 9:
                int size9 = readVInt();
                Map map9 = new LinkedHashMap(size9);
                for (int i = 0; i < size9; i++) {
                    map9.put(readString(), readGenericValue());
                }
                return map9;
            case 10:
                int size10 = readVInt();
                Map map10 = new HashMap(size10);
                for (int i = 0; i < size10; i++) {
                    map10.put(readString(), readGenericValue());
                }
                return map10;
            case 11:
                return readByte();
            case 12:
                return new Date(readLong());
            case 13:
                final String timeZoneId = readString();
                return new DateTime(readLong(), DateTimeZone.forID(timeZoneId));
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
            default:
                throw new IOException("Can't read unknown type [" + type + "]");
        }
    }

    /**
     * Reads a {@link GeoPoint} from this stream input
     */
    public GeoPoint readGeoPoint() throws IOException {
        return new GeoPoint(readDouble(), readDouble());
    }

    public int[] readIntArray() throws IOException {
        int length = readVInt();
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = readInt();
        }
        return values;
    }

    public int[] readVIntArray() throws IOException {
        int length = readVInt();
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = readVInt();
        }
        return values;
    }

    public long[] readLongArray() throws IOException {
        int length = readVInt();
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = readLong();
        }
        return values;
    }

    public long[] readVLongArray() throws IOException {
        int length = readVInt();
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = readVLong();
        }
        return values;
    }

    public float[] readFloatArray() throws IOException {
        int length = readVInt();
        float[] values = new float[length];
        for (int i = 0; i < length; i++) {
            values[i] = readFloat();
        }
        return values;
    }

    public double[] readDoubleArray() throws IOException {
        int length = readVInt();
        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = readDouble();
        }
        return values;
    }

    public byte[] readByteArray() throws IOException {
        int length = readVInt();
        byte[] values = new byte[length];
        for (int i = 0; i < length; i++) {
            values[i] = readByte();
        }
        return values;
    }

    /**
     * Serializes a potential null value.
     */
    public <T extends Streamable> T readOptionalStreamable(Supplier<T> supplier) throws IOException {
        if (readBoolean()) {
            T streamable = supplier.get();
            streamable.readFrom(this);
            return streamable;
        } else {
            return null;
        }
    }

    public <T extends Throwable> T readThrowable() throws IOException {
        if (readBoolean()) {
            int key = readVInt();
            switch (key) {
                case 0:
                    final int ord = readVInt();
                    return (T) readException(this, ord);
                case 1:
                    String msg1 = readOptionalString();
                    String resource1 = readOptionalString();
                    return (T) readStackTrace(new CorruptIndexException(msg1, resource1, readThrowable()), this);
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
                    return (T) readStackTrace(new IllegalArgumentException(readOptionalString(), readThrowable()), this);
                case 7:
                    return (T) readStackTrace(new AlreadyClosedException(readOptionalString(), readThrowable()), this);
                case 8:
                    return (T) readStackTrace(new EOFException(readOptionalString()), this);
                case 9:
                    return (T) readStackTrace(new SecurityException(readOptionalString(), readThrowable()), this);
                case 10:
                    return (T) readStackTrace(new StringIndexOutOfBoundsException(readOptionalString()), this);
                case 11:
                    return (T) readStackTrace(new ArrayIndexOutOfBoundsException(readOptionalString()), this);
                case 12:
                    return (T) readStackTrace(new AssertionError(readOptionalString(), readThrowable()), this);
                case 13:
                    return (T) readStackTrace(new FileNotFoundException(readOptionalString()), this);
                case 14:
                    final String file = readOptionalString();
                    final String other = readOptionalString();
                    final String reason = readOptionalString();
                    readOptionalString(); // skip the msg - it's composed from file, other and reason
                    return (T) readStackTrace(new NoSuchFileException(file, other, reason), this);
                case 15:
                    return (T) readStackTrace(new OutOfMemoryError(readOptionalString()), this);
                case 16:
                    return (T) readStackTrace(new IllegalStateException(readOptionalString(), readThrowable()), this);
                case 17:
                    return (T) readStackTrace(new LockObtainFailedException(readOptionalString(), readThrowable()), this);
                case 18:
                    return (T) readStackTrace(new InterruptedException(readOptionalString()), this);
                default:
                    assert false : "no such exception for id: " + key;
            }
        }
        return null;
    }

    /**
     * Reads a {@link NamedWriteable} from the current stream, by first reading its name and then looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     * Use {@link FilterInputStream} instead which wraps a stream and supports a {@link NamedWriteableRegistry} too.
     */
    <C> C readNamedWriteable(@SuppressWarnings("unused") Class<C> categoryClass) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads a {@link QueryBuilder} from the current stream
     */
    public QueryBuilder readQuery() throws IOException {
        return readNamedWriteable(QueryBuilder.class);
    }

    /**
     * Reads a {@link org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder} from the current stream
     */
    public ScoreFunctionBuilder<?> readScoreFunction() throws IOException {
        return readNamedWriteable(ScoreFunctionBuilder.class);
    }

    public static StreamInput wrap(BytesReference reference) {
        if (reference.hasArray() == false) {
            reference = reference.toBytesArray();
        }
        return wrap(reference.array(), reference.arrayOffset(), reference.length());
    }

    public static StreamInput wrap(byte[] bytes) {
        return wrap(bytes, 0, bytes.length);
    }

    public static StreamInput wrap(byte[] bytes, int offset, int length) {
        return new InputStreamStreamInput(new ByteArrayInputStream(bytes, offset, length));
    }
}
