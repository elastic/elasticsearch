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

import com.vividsolutions.jts.util.Assert;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.text.Text;
import org.joda.time.ReadableInstant;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public abstract class StreamOutput extends OutputStream {

    private Version version = Version.CURRENT;

    public Version getVersion() {
        return this.version;
    }

    public StreamOutput setVersion(Version version) {
        this.version = version;
        return this;
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
     * Writes an long in a variable-length format.  Writes between one and nine
     * bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    public void writeVLong(long i) throws IOException {
        assert i >= 0;
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
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

    public void writeMap(@Nullable Map<String, Object> map) throws IOException {
        writeGenericValue(map);
    }

    public void writeGenericValue(@Nullable Object value) throws IOException {
        if (value == null) {
            writeByte((byte) -1);
            return;
        }
        Class type = value.getClass();
        if (type == String.class) {
            writeByte((byte) 0);
            writeString((String) value);
        } else if (type == Integer.class) {
            writeByte((byte) 1);
            writeInt((Integer) value);
        } else if (type == Long.class) {
            writeByte((byte) 2);
            writeLong((Long) value);
        } else if (type == Float.class) {
            writeByte((byte) 3);
            writeFloat((Float) value);
        } else if (type == Double.class) {
            writeByte((byte) 4);
            writeDouble((Double) value);
        } else if (type == Boolean.class) {
            writeByte((byte) 5);
            writeBoolean((Boolean) value);
        } else if (type == byte[].class) {
            writeByte((byte) 6);
            writeVInt(((byte[]) value).length);
            writeBytes(((byte[]) value));
        } else if (value instanceof List) {
            writeByte((byte) 7);
            List list = (List) value;
            writeVInt(list.size());
            for (Object o : list) {
                writeGenericValue(o);
            }
        } else if (value instanceof Object[]) {
            writeByte((byte) 8);
            Object[] list = (Object[]) value;
            writeVInt(list.length);
            for (Object o : list) {
                writeGenericValue(o);
            }
        } else if (value instanceof Map) {
            if (value instanceof LinkedHashMap) {
                writeByte((byte) 9);
            } else {
                writeByte((byte) 10);
            }
            Map<String, Object> map = (Map<String, Object>) value;
            writeVInt(map.size());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                writeString(entry.getKey());
                writeGenericValue(entry.getValue());
            }
        } else if (type == Byte.class) {
            writeByte((byte) 11);
            writeByte((Byte) value);
        } else if (type == Date.class) {
            writeByte((byte) 12);
            writeLong(((Date) value).getTime());
        } else if (value instanceof ReadableInstant) {
            writeByte((byte) 13);
            writeString(((ReadableInstant) value).getZone().getID());
            writeLong(((ReadableInstant) value).getMillis());
        } else if (value instanceof BytesReference) {
            writeByte((byte) 14);
            writeBytesReference((BytesReference) value);
        } else if (value instanceof Text) {
            writeByte((byte) 15);
            writeText((Text) value);
        } else if (type == Short.class) {
            writeByte((byte) 16);
            writeShort((Short) value);
        } else if (type == int[].class) {
            writeByte((byte) 17);
            writeIntArray((int[]) value);
        } else if (type == long[].class) {
            writeByte((byte) 18);
            writeLongArray((long[]) value);
        } else if (type == float[].class) {
            writeByte((byte) 19);
            writeFloatArray((float[]) value);
        } else if (type == double[].class) {
            writeByte((byte) 20);
            writeDoubleArray((double[]) value);
        } else if (value instanceof BytesRef) {
            writeByte((byte) 21);
            writeBytesRef((BytesRef) value);
        } else {
            throw new IOException("Can't write type [" + type + "]");
        }
    }

    public void writeIntArray(int[] value) throws IOException {
        writeVInt(value.length);
        for (int i=0; i<value.length; i++) {
            writeInt(value[i]);
        }
    }

    public void writeLongArray(long[] value) throws IOException {
        writeVInt(value.length);
        for (int i=0; i<value.length; i++) {
            writeLong(value[i]);
        }
    }

    public void writeFloatArray(float[] value) throws IOException {
        writeVInt(value.length);
        for (int i=0; i<value.length; i++) {
            writeFloat(value[i]);
        }
    }

    public void writeDoubleArray(double[] value) throws IOException {
        writeVInt(value.length);
        for (int i=0; i<value.length; i++) {
            writeDouble(value[i]);
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

    static {
        assert Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_5_2_1: "Remove these regex once we upgrade to Lucene 5.3 and get proper getters for these expections";
    }
    private final static Pattern CORRUPT_INDEX_EXCEPTION_REGEX = Regex.compile("^(.+) \\(resource=(.+)\\)$", "");
    private final static Pattern INDEX_FORMAT_TOO_NEW_EXCEPTION_REGEX = Regex.compile("Format version is not supported \\(resource (.+)\\): (-?\\d+) \\(needs to be between (-?\\d+) and (-?\\d+)\\)", "");
    private final static Pattern INDEX_FORMAT_TOO_OLD_EXCEPTION_REGEX_1 = Regex.compile("Format version is not supported \\(resource (.+)\\): (-?\\d+)(?: \\(needs to be between (-?\\d+) and (-?\\d+)\\)). This version of Lucene only supports indexes created with release 4.0 and later\\.", "");
    private final static Pattern INDEX_FORMAT_TOO_OLD_EXCEPTION_REGEX_2 = Regex.compile("Format version is not supported \\(resource (.+)\\): (.+). This version of Lucene only supports indexes created with release 4.0 and later\\.", "");

    private static int parseIntSafe(String val, int defaultVal) {
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException ex) {
            return defaultVal;
        }
    }

    public void writeThrowable(Throwable throwable) throws IOException {
        if (throwable == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            boolean writeCause = true;
            boolean writeMessage = true;
            if (throwable instanceof CorruptIndexException) {
                writeVInt(1);
                // Lucene 5.3 will have getters for all these
                // we should switch to using getters instead of trying to parse the message:
                // writeOptionalString(((CorruptIndexException)throwable).getDescription());
                // writeOptionalString(((CorruptIndexException)throwable).getResource());
                Matcher matcher = CORRUPT_INDEX_EXCEPTION_REGEX.matcher(throwable.getMessage());
                if (matcher.find()) {
                    writeOptionalString(matcher.group(1)); // message
                    writeOptionalString(matcher.group(2)); // resource
                } else {
                    // didn't match
                    writeOptionalString("???"); // message
                    writeOptionalString("???"); // resource
                }
                writeMessage = false;
            } else if (throwable instanceof IndexFormatTooNewException) {
                writeVInt(2);
                // Lucene 5.3 will have getters for all these
                // we should switch to using getters instead of trying to parse the message:
                // writeOptionalString(((CorruptIndexException)throwable).getResource());
                // writeInt(((IndexFormatTooNewException)throwable).getVersion());
                // writeInt(((IndexFormatTooNewException)throwable).getMinVersion());
                // writeInt(((IndexFormatTooNewException)throwable).getMaxVersion());
                Matcher matcher = INDEX_FORMAT_TOO_NEW_EXCEPTION_REGEX.matcher(throwable.getMessage());
                if (matcher.find()) {
                    writeOptionalString(matcher.group(1)); // resource
                    writeInt(parseIntSafe(matcher.group(2), -1)); // version
                    writeInt(parseIntSafe(matcher.group(3), -1)); // min version
                    writeInt(parseIntSafe(matcher.group(4), -1)); // max version
                } else {
                    // didn't match
                    writeOptionalString("???"); // resource
                    writeInt(-1); // version
                    writeInt(-1); // min version
                    writeInt(-1); // max version
                }
                writeMessage = false;
                writeCause = false;
            } else if (throwable instanceof IndexFormatTooOldException) {
                writeVInt(3);
                // Lucene 5.3 will have getters for all these
                // we should switch to using getters instead of trying to parse the message:
                // writeOptionalString(((CorruptIndexException)throwable).getResource());
                // writeInt(((IndexFormatTooNewException)throwable).getVersion());
                // writeInt(((IndexFormatTooNewException)throwable).getMinVersion());
                // writeInt(((IndexFormatTooNewException)throwable).getMaxVersion());
                Matcher matcher = INDEX_FORMAT_TOO_OLD_EXCEPTION_REGEX_1.matcher(throwable.getMessage());
                if (matcher.find()) {
                    // version with numeric version in constructor
                    writeOptionalString(matcher.group(1)); // resource
                    writeBoolean(true);
                    writeInt(parseIntSafe(matcher.group(2), -1)); // version
                    writeInt(parseIntSafe(matcher.group(3), -1)); // min version
                    writeInt(parseIntSafe(matcher.group(4), -1)); // max version
                } else {
                    matcher = INDEX_FORMAT_TOO_OLD_EXCEPTION_REGEX_2.matcher(throwable.getMessage());
                    if (matcher.matches()) {
                        writeOptionalString(matcher.group(1)); // resource
                        writeBoolean(false);
                        writeOptionalString(matcher.group(2)); // version
                    } else {
                        // didn't match
                        writeOptionalString("???"); // resource
                        writeBoolean(true);
                        writeInt(-1); // version
                        writeInt(-1); // min version
                        writeInt(-1); // max version
                    }
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
            } else if (throwable instanceof AssertionError) {
                writeVInt(12);
            } else if (throwable instanceof FileNotFoundException) {
                writeVInt(13);
                writeCause = false;
            } else if (throwable instanceof NoSuchFileException) {
                writeVInt(14);
                writeOptionalString(((NoSuchFileException) throwable).getFile());
                writeOptionalString(((NoSuchFileException) throwable).getOtherFile());
                writeOptionalString(((NoSuchFileException) throwable).getReason());
                writeCause = false;
            } else if (throwable instanceof OutOfMemoryError) {
                writeVInt(15);
                writeCause = false;
            } else if (throwable instanceof IllegalStateException) {
                writeVInt(16);
            } else if (throwable instanceof LockObtainFailedException) {
                writeVInt(17);
            } else {
                ElasticsearchException ex;
                final String name = throwable.getClass().getName();
                if (throwable instanceof ElasticsearchException && ElasticsearchException.isRegistered(name)) {
                    ex = (ElasticsearchException) throwable;
                } else {
                    ex = new NotSerializableExceptionWrapper(throwable);
                }
                writeVInt(0);
                writeString(ex.getClass().getName());
                ex.writeTo(this);
                return;

            }
            if (writeMessage) {
                writeOptionalString(throwable.getMessage());
            }
            if (writeCause) {
                writeThrowable(throwable.getCause());
            }
            ElasticsearchException.writeStackTraces(throwable, this);
        }
    }
}
