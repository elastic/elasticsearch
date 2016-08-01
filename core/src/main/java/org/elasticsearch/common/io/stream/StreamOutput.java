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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.Text;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A stream from another node to this node. Technically, it can also be streamed from a byte array but that is mostly for testing.
 *
 * This class's methods are optimized so you can put the methods that read and write a class next to each other and you can scan them
 * visually for differences. That means that most variables should be read and written in a single line so even large objects fit both
 * reading and writing on the screen. It also means that the methods on this class are named very similarly to {@link StreamInput}. Finally
 * it means that the "barrier to entry" for adding new methods to this class is relatively low even though it is a shared class with code
 * everywhere. That being said, this class deals primarily with {@code List}s rather than Arrays. For the most part calls should adapt to
 * lists, either by storing {@code List}s internally or just converting to and from a {@code List} when calling. This comment is repeated
 * on {@link StreamInput}.
 */
public abstract class StreamOutput extends OutputStream {

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

    public long position() throws IOException {
        throw new UnsupportedOperationException();
    }

    public void seek(long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes a single byte.
     */
    public abstract void writeByte(byte b) throws IOException;

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     */
    public void writeBytes(byte[] b) throws IOException {
        writeBytes(b, 0, b.length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param length the number of bytes to write
     */
    public void writeBytes(byte[] b, int length) throws IOException {
        writeBytes(b, 0, length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     */
    public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     */
    public void writeByteArray(byte[] b) throws IOException {
        writeVInt(b.length);
        writeBytes(b, 0, b.length);
    }

    /**
     * Writes the bytes reference, including a length header.
     */
    public void writeBytesReference(@Nullable BytesReference bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
            return;
        }
        writeVInt(bytes.length());
        bytes.writeTo(this);
    }

    /**
     * Writes an optional bytes reference including a length header. Use this if you need to differentiate between null and empty bytes
     * references. Use {@link #writeBytesReference(BytesReference)} and {@link StreamInput#readBytesReference()} if you do not.
     */
    public void writeOptionalBytesReference(@Nullable BytesReference bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
            return;
        }
        writeVInt(bytes.length() + 1);
        bytes.writeTo(this);
    }

    public void writeBytesRef(BytesRef bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
            return;
        }
        writeVInt(bytes.length);
        write(bytes.bytes, bytes.offset, bytes.length);
    }

    public final void writeShort(short v) throws IOException {
        writeByte((byte) (v >> 8));
        writeByte((byte) v);
    }

    /**
     * Writes an int as four bytes.
     */
    public void writeInt(int i) throws IOException {
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    /**
     * Writes an int in a variable-length format.  Writes between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #writeInt}
     */
    public void writeVInt(int i) throws IOException {
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    /**
     * Writes a long as eight bytes.
     */
    public void writeLong(long i) throws IOException {
        writeInt((int) (i >> 32));
        writeInt((int) i);
    }

    /**
     * Writes a non-negative long in a variable-length format.
     * Writes between one and nine bytes. Smaller values take fewer bytes.
     * Negative numbers are not supported.
     */
    public void writeVLong(long i) throws IOException {
        assert i >= 0;
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    /**
     * Writes a long in a variable-length format. Writes between one and ten bytes.
     * Values are remapped by sliding the sign bit into the lsb and then encoded as an unsigned number
     * e.g., 0 -;&gt; 0, -1 -;&gt; 1, 1 -;&gt; 2, ..., Long.MIN_VALUE -;&gt; -1, Long.MAX_VALUE -;&gt; -2
     * Numbers with small absolute value will have a small encoding
     * If the numbers are known to be non-negative, use {@link #writeVLong(long)}
     */
    public void writeZLong(long i) throws IOException {
        // zig-zag encoding cf. https://developers.google.com/protocol-buffers/docs/encoding?hl=en
        long value = BitUtil.zigZagEncode(i);
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            writeByte((byte)((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte((byte) (value & 0x7F));
    }

    public void writeOptionalLong(@Nullable Long l) throws IOException {
        if (l == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeLong(l);
        }
    }

    public void writeOptionalString(@Nullable String str) throws IOException {
        if (str == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeString(str);
        }
    }

    public void writeOptionalVInt(@Nullable Integer integer) throws IOException {
        if (integer == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeVInt(integer);
        }
    }

    public void writeOptionalFloat(@Nullable Float floatValue) throws IOException {
        if (floatValue == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeFloat(floatValue);
        }
    }

    public void writeOptionalText(@Nullable Text text) throws IOException {
        if (text == null) {
            writeInt(-1);
        } else {
            writeText(text);
        }
    }

    private final BytesRefBuilder spare = new BytesRefBuilder();

    public void writeText(Text text) throws IOException {
        if (!text.hasBytes()) {
            final String string = text.string();
            spare.copyChars(string);
            writeInt(spare.length());
            write(spare.bytes(), 0, spare.length());
        } else {
            BytesReference bytes = text.bytes();
            writeInt(bytes.length());
            bytes.writeTo(this);
        }
    }

    public void writeString(String str) throws IOException {
        int charCount = str.length();
        writeVInt(charCount);
        int c;
        for (int i = 0; i < charCount; i++) {
            c = str.charAt(i);
            if (c <= 0x007F) {
                writeByte((byte) c);
            } else if (c > 0x07FF) {
                writeByte((byte) (0xE0 | c >> 12 & 0x0F));
                writeByte((byte) (0x80 | c >> 6 & 0x3F));
                writeByte((byte) (0x80 | c >> 0 & 0x3F));
            } else {
                writeByte((byte) (0xC0 | c >> 6 & 0x1F));
                writeByte((byte) (0x80 | c >> 0 & 0x3F));
            }
        }
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeOptionalDouble(@Nullable Double v) throws IOException {
        if (v == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeDouble(v);
        }
    }

    private static byte ZERO = 0;
    private static byte ONE = 1;
    private static byte TWO = 2;

    /**
     * Writes a boolean.
     */
    public void writeBoolean(boolean b) throws IOException {
        writeByte(b ? ONE : ZERO);
    }

    public void writeOptionalBoolean(@Nullable Boolean b) throws IOException {
        if (b == null) {
            writeByte(TWO);
        } else {
            writeByte(b ? ONE : ZERO);
        }
    }

    /**
     * Forces any buffered output to be written.
     */
    @Override
    public abstract void flush() throws IOException;

    /**
     * Closes this stream to further operations.
     */
    @Override
    public abstract void close() throws IOException;

    public abstract void reset() throws IOException;

    @Override
    public void write(int b) throws IOException {
        writeByte((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writeBytes(b, off, len);
    }

    public void writeStringArray(String[] array) throws IOException {
        writeVInt(array.length);
        for (String s : array) {
            writeString(s);
        }
    }

    /**
     * Writes a string array, for nullable string, writes it as 0 (empty string).
     */
    public void writeStringArrayNullable(@Nullable String[] array) throws IOException {
        if (array == null) {
            writeVInt(0);
        } else {
            writeVInt(array.length);
            for (String s : array) {
                writeString(s);
            }
        }
    }

    /**
     * Writes a string array, for nullable string, writes false.
     */
    public void writeOptionalStringArray(@Nullable String[] array) throws IOException {
        if (array == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeStringArray(array);
        }
    }

    public void writeMap(@Nullable Map<String, Object> map) throws IOException {
        writeGenericValue(map);
    }

    /**
     * Writes a map of strings to string lists.
     */
    public void writeMapOfLists(Map<String, List<String>> map) throws IOException {
        writeVInt(map.size());

        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            writeString(entry.getKey());
            writeVInt(entry.getValue().size());
            for (String v : entry.getValue()) {
                writeString(v);
            }
        }
    }

    @FunctionalInterface
    interface Writer {
        void write(StreamOutput o, Object value) throws IOException;
    }

    private static final Map<Class<?>, Writer> WRITERS;

    static {
        Map<Class<?>, Writer> writers = new HashMap<>();
        writers.put(String.class, (o, v) -> {
            o.writeByte((byte) 0);
            o.writeString((String) v);
        });
        writers.put(Integer.class, (o, v) -> {
            o.writeByte((byte) 1);
            o.writeInt((Integer) v);
        });
        writers.put(Long.class, (o, v) -> {
            o.writeByte((byte) 2);
            o.writeLong((Long) v);
        });
        writers.put(Float.class, (o, v) -> {
            o.writeByte((byte) 3);
            o.writeFloat((float) v);
        });
        writers.put(Double.class, (o, v) -> {
            o.writeByte((byte) 4);
            o.writeDouble((double) v);
        });
        writers.put(Boolean.class, (o, v) -> {
            o.writeByte((byte) 5);
            o.writeBoolean((boolean) v);
        });
        writers.put(byte[].class, (o, v) -> {
            o.writeByte((byte) 6);
            final byte[] bytes = (byte[]) v;
            o.writeVInt(bytes.length);
            o.writeBytes(bytes);
        });
        writers.put(List.class, (o, v) -> {
            o.writeByte((byte) 7);
            final List list = (List) v;
            o.writeVInt(list.size());
            for (Object item : list) {
                o.writeGenericValue(item);
            }
        });
        writers.put(Object[].class, (o, v) -> {
            o.writeByte((byte) 8);
            final Object[] list = (Object[]) v;
            o.writeVInt(list.length);
            for (Object item : list) {
                o.writeGenericValue(item);
            }
        });
        writers.put(Map.class, (o, v) -> {
            if (v instanceof LinkedHashMap) {
                o.writeByte((byte) 9);
            } else {
                o.writeByte((byte) 10);
            }
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = (Map<String, Object>) v;
            o.writeVInt(map.size());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                o.writeString(entry.getKey());
                o.writeGenericValue(entry.getValue());
            }
        });
        writers.put(Byte.class, (o, v) -> {
            o.writeByte((byte) 11);
            o.writeByte((Byte) v);
        });
        writers.put(Date.class, (o, v) -> {
            o.writeByte((byte) 12);
            o.writeLong(((Date) v).getTime());
        });
        writers.put(ReadableInstant.class, (o, v) -> {
            o.writeByte((byte) 13);
            final ReadableInstant instant = (ReadableInstant) v;
            o.writeString(instant.getZone().getID());
            o.writeLong(instant.getMillis());
        });
        writers.put(BytesReference.class, (o, v) -> {
            o.writeByte((byte) 14);
            o.writeBytesReference((BytesReference) v);
        });
        writers.put(Text.class, (o, v) -> {
            o.writeByte((byte) 15);
            o.writeText((Text) v);
        });
        writers.put(Short.class, (o, v) -> {
            o.writeByte((byte) 16);
            o.writeShort((Short) v);
        });
        writers.put(int[].class, (o, v) -> {
            o.writeByte((byte) 17);
            o.writeIntArray((int[]) v);
        });
        writers.put(long[].class, (o, v) -> {
            o.writeByte((byte) 18);
            o.writeLongArray((long[]) v);
        });
        writers.put(float[].class, (o, v) -> {
            o.writeByte((byte) 19);
            o.writeFloatArray((float[]) v);
        });
        writers.put(double[].class, (o, v) -> {
            o.writeByte((byte) 20);
            o.writeDoubleArray((double[]) v);
        });
        writers.put(BytesRef.class, (o, v) -> {
            o.writeByte((byte) 21);
            o.writeBytesRef((BytesRef) v);
        });
        writers.put(GeoPoint.class, (o, v) -> {
            o.writeByte((byte) 22);
            o.writeGeoPoint((GeoPoint) v);
        });
        WRITERS = Collections.unmodifiableMap(writers);
    }

    public void writeGenericValue(@Nullable Object value) throws IOException {
        if (value == null) {
            writeByte((byte) -1);
            return;
        }
        final Class type;
        if (value instanceof List) {
            type = List.class;
        } else if (value instanceof Object[]) {
            type = Object[].class;
        } else if (value instanceof Map) {
            type = Map.class;
        } else if (value instanceof ReadableInstant) {
            type = ReadableInstant.class;
        } else if (value instanceof BytesReference) {
            type = BytesReference.class;
        } else {
            type = value.getClass();
        }
        final Writer writer = WRITERS.get(type);
        if (writer != null) {
            writer.write(this, value);
        } else {
            throw new IOException("can not write type [" + type + "]");
        }
    }

    public void writeIntArray(int[] values) throws IOException {
        writeVInt(values.length);
        for (int value : values) {
            writeInt(value);
        }
    }

    public void writeVIntArray(int[] values) throws IOException {
        writeVInt(values.length);
        for (int value : values) {
            writeVInt(value);
        }
    }

    public void writeLongArray(long[] values) throws IOException {
        writeVInt(values.length);
        for (long value : values) {
            writeLong(value);
        }
    }

    public void writeVLongArray(long[] values) throws IOException {
        writeVInt(values.length);
        for (long value : values) {
            writeVLong(value);
        }
    }

    public void writeFloatArray(float[] values) throws IOException {
        writeVInt(values.length);
        for (float value : values) {
            writeFloat(value);
        }
    }

    public void writeDoubleArray(double[] values) throws IOException {
        writeVInt(values.length);
        for (double value : values) {
            writeDouble(value);
        }
    }

    public <T extends Writeable> void writeArray(T[] array) throws IOException {
        writeVInt(array.length);
        for (T value: array) {
            value.writeTo(this);
        }
    }

    public <T extends Writeable> void writeOptionalArray(@Nullable T[] array) throws IOException {
        if (array == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeArray(array);
        }
    }

    /**
     * Serializes a potential null value.
     */
    public void writeOptionalStreamable(@Nullable Streamable streamable) throws IOException {
        if (streamable != null) {
            writeBoolean(true);
            streamable.writeTo(this);
        } else {
            writeBoolean(false);
        }
    }

    public void writeOptionalWriteable(@Nullable Writeable writeable) throws IOException {
        if (writeable != null) {
            writeBoolean(true);
            writeable.writeTo(this);
        } else {
            writeBoolean(false);
        }
    }

    public void writeException(Throwable throwable) throws IOException {
        if (throwable == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            boolean writeCause = true;
            boolean writeMessage = true;
            if (throwable instanceof CorruptIndexException) {
                writeVInt(1);
                writeOptionalString(((CorruptIndexException)throwable).getOriginalMessage());
                writeOptionalString(((CorruptIndexException)throwable).getResourceDescription());
                writeMessage = false;
            } else if (throwable instanceof IndexFormatTooNewException) {
                writeVInt(2);
                writeOptionalString(((IndexFormatTooNewException)throwable).getResourceDescription());
                writeInt(((IndexFormatTooNewException)throwable).getVersion());
                writeInt(((IndexFormatTooNewException)throwable).getMinVersion());
                writeInt(((IndexFormatTooNewException)throwable).getMaxVersion());
                writeMessage = false;
                writeCause = false;
            } else if (throwable instanceof IndexFormatTooOldException) {
                writeVInt(3);
                IndexFormatTooOldException t = (IndexFormatTooOldException) throwable;
                writeOptionalString(t.getResourceDescription());
                if (t.getVersion() == null) {
                    writeBoolean(false);
                    writeOptionalString(t.getReason());
                } else {
                    writeBoolean(true);
                    writeInt(t.getVersion());
                    writeInt(t.getMinVersion());
                    writeInt(t.getMaxVersion());
                }
                writeMessage = false;
                writeCause = false;
            } else if (throwable instanceof NullPointerException) {
                writeVInt(4);
                writeCause = false;
            } else if (throwable instanceof NumberFormatException) {
                writeVInt(5);
                writeCause = false;
            } else if (throwable instanceof IllegalArgumentException) {
                writeVInt(6);
            } else if (throwable instanceof AlreadyClosedException) {
                writeVInt(7);
            } else if (throwable instanceof EOFException) {
                writeVInt(8);
                writeCause = false;
            } else if (throwable instanceof SecurityException) {
                writeVInt(9);
            } else if (throwable instanceof StringIndexOutOfBoundsException) {
                writeVInt(10);
                writeCause = false;
            } else if (throwable instanceof ArrayIndexOutOfBoundsException) {
                writeVInt(11);
                writeCause = false;
            } else if (throwable instanceof FileNotFoundException) {
                writeVInt(12);
                writeCause = false;
            } else if (throwable instanceof FileSystemException) {
                writeVInt(13);
                if (throwable instanceof NoSuchFileException) {
                    writeVInt(0);
                } else if (throwable instanceof NotDirectoryException) {
                    writeVInt(1);
                } else if (throwable instanceof DirectoryNotEmptyException) {
                    writeVInt(2);
                } else if (throwable instanceof AtomicMoveNotSupportedException) {
                    writeVInt(3);
                } else if (throwable instanceof FileAlreadyExistsException) {
                    writeVInt(4);
                } else if (throwable instanceof AccessDeniedException) {
                    writeVInt(5);
                } else if (throwable instanceof FileSystemLoopException) {
                    writeVInt(6);
                } else {
                    writeVInt(7);
                }
                writeOptionalString(((FileSystemException) throwable).getFile());
                writeOptionalString(((FileSystemException) throwable).getOtherFile());
                writeOptionalString(((FileSystemException) throwable).getReason());
                writeCause = false;
            } else if (throwable instanceof IllegalStateException) {
                writeVInt(14);
            } else if (throwable instanceof LockObtainFailedException) {
                writeVInt(15);
            } else if (throwable instanceof InterruptedException) {
                writeVInt(16);
                writeCause = false;
            } else if (throwable instanceof IOException) {
                writeVInt(17);
            } else {
                ElasticsearchException ex;
                if (throwable instanceof ElasticsearchException && ElasticsearchException.isRegistered(throwable.getClass())) {
                    ex = (ElasticsearchException) throwable;
                } else {
                    ex = new NotSerializableExceptionWrapper(throwable);
                }
                writeVInt(0);
                writeVInt(ElasticsearchException.getId(ex.getClass()));
                ex.writeTo(this);
                return;

            }
            if (writeMessage) {
                writeOptionalString(throwable.getMessage());
            }
            if (writeCause) {
                writeException(throwable.getCause());
            }
            ElasticsearchException.writeStackTraces(throwable, this);
        }
    }

    /**
     * Writes a {@link NamedWriteable} to the current stream, by first writing its name and then the object itself
     */
    public void writeNamedWriteable(NamedWriteable namedWriteable) throws IOException {
        writeString(namedWriteable.getWriteableName());
        namedWriteable.writeTo(this);
    }

    /**
     * Write an optional {@link NamedWriteable} to the stream.
     */
    public void writeOptionalNamedWriteable(@Nullable NamedWriteable namedWriteable) throws IOException {
        if (namedWriteable == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeNamedWriteable(namedWriteable);
        }
    }

    /**
     * Writes the given {@link GeoPoint} to the stream
     */
    public void writeGeoPoint(GeoPoint geoPoint) throws IOException {
        writeDouble(geoPoint.lat());
        writeDouble(geoPoint.lon());
    }

    /**
     * Write a {@linkplain DateTimeZone} to the stream.
     */
    public void writeTimeZone(DateTimeZone timeZone) throws IOException {
        writeString(timeZone.getID());
    }

    /**
     * Write an optional {@linkplain DateTimeZone} to the stream.
     */
    public void writeOptionalTimeZone(@Nullable DateTimeZone timeZone) throws IOException {
        if (timeZone == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeTimeZone(timeZone);
        }
    }

    /**
     * Writes a list of {@link Streamable} objects
     */
    public void writeStreamableList(List<? extends Streamable> list) throws IOException {
        writeVInt(list.size());
        for (Streamable obj: list) {
            obj.writeTo(this);
        }
    }

    /**
     * Writes a list of {@link Writeable} objects
     */
    public void writeList(List<? extends Writeable> list) throws IOException {
        writeVInt(list.size());
        for (Writeable obj: list) {
            obj.writeTo(this);
        }
    }

    /**
     * Writes a list of {@link NamedWriteable} objects.
     */
    public void writeNamedWriteableList(List<? extends NamedWriteable> list) throws IOException {
        writeVInt(list.size());
        for (NamedWriteable obj: list) {
            writeNamedWriteable(obj);
        }
    }
}
