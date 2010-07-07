package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.text.DecimalFormat;

/**
 * Expert: Maintains caches of term values.
 *
 * <p>Created: May 19, 2004 11:13:14 AM
 *
 * @see org.apache.lucene.util.FieldCacheSanityChecker
 * @since lucene 1.4
 */
// LUCENE MONITOR - Added the ability to listen to purge notifications, sadly no extension point in Lucene for this...
public interface FieldCache {

    void setIndexReaderPurgedListener(IndexReaderPurgedListener listener);

    public static final class CreationPlaceholder {
        Object value;
    }

    /**
     * Indicator for StringIndex values in the cache.
     */
    // NOTE: the value assigned to this constant must not be
    // the same as any of those in SortField!!
    public static final int STRING_INDEX = -1;


    /**
     * Expert: Stores term text values and document ordering data.
     */
    public static class StringIndex {

        public int binarySearchLookup(String key) {
            // this special case is the reason that Arrays.binarySearch() isn't useful.
            if (key == null)
                return 0;

            int low = 1;
            int high = lookup.length - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = lookup[mid].compareTo(key);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }

        /**
         * All the term values, in natural order.
         */
        public final String[] lookup;

        /**
         * For each document, an index into the lookup array.
         */
        public final int[] order;

        /**
         * Creates one of these objects
         */
        public StringIndex(int[] values, String[] lookup) {
            this.order = values;
            this.lookup = lookup;
        }
    }

    /**
     * Marker interface as super-interface to all parsers. It
     * is used to specify a custom parser to {@link
     * SortField#SortField(String, FieldCache.Parser)}.
     */
    public interface Parser extends Serializable {
    }

    /**
     * Interface to parse bytes from document fields.
     *
     * @see FieldCache#getBytes(IndexReader, String, FieldCache.ByteParser)
     */
    public interface ByteParser extends Parser {
        /**
         * Return a single Byte representation of this field's value.
         */
        public byte parseByte(String string);
    }

    /**
     * Interface to parse shorts from document fields.
     *
     * @see FieldCache#getShorts(IndexReader, String, FieldCache.ShortParser)
     */
    public interface ShortParser extends Parser {
        /**
         * Return a short representation of this field's value.
         */
        public short parseShort(String string);
    }

    /**
     * Interface to parse ints from document fields.
     *
     * @see FieldCache#getInts(IndexReader, String, FieldCache.IntParser)
     */
    public interface IntParser extends Parser {
        /**
         * Return an integer representation of this field's value.
         */
        public int parseInt(String string);
    }

    /**
     * Interface to parse floats from document fields.
     *
     * @see FieldCache#getFloats(IndexReader, String, FieldCache.FloatParser)
     */
    public interface FloatParser extends Parser {
        /**
         * Return an float representation of this field's value.
         */
        public float parseFloat(String string);
    }

    /**
     * Interface to parse long from document fields.
     *
     * @see FieldCache#getLongs(IndexReader, String, FieldCache.LongParser)
     */
    public interface LongParser extends Parser {
        /**
         * Return an long representation of this field's value.
         */
        public long parseLong(String string);
    }

    /**
     * Interface to parse doubles from document fields.
     *
     * @see FieldCache#getDoubles(IndexReader, String, FieldCache.DoubleParser)
     */
    public interface DoubleParser extends Parser {
        /**
         * Return an long representation of this field's value.
         */
        public double parseDouble(String string);
    }

    /**
     * Expert: The cache used internally by sorting and range query classes.
     */
    public static FieldCache DEFAULT = new FieldCacheImpl();

    /**
     * The default parser for byte values, which are encoded by {@link Byte#toString(byte)}
     */
    public static final ByteParser DEFAULT_BYTE_PARSER = new ByteParser() {
        public byte parseByte(String value) {
            return Byte.parseByte(value);
        }

        protected Object readResolve() {
            return DEFAULT_BYTE_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".DEFAULT_BYTE_PARSER";
        }
    };

    /**
     * The default parser for short values, which are encoded by {@link Short#toString(short)}
     */
    public static final ShortParser DEFAULT_SHORT_PARSER = new ShortParser() {
        public short parseShort(String value) {
            return Short.parseShort(value);
        }

        protected Object readResolve() {
            return DEFAULT_SHORT_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".DEFAULT_SHORT_PARSER";
        }
    };

    /**
     * The default parser for int values, which are encoded by {@link Integer#toString(int)}
     */
    public static final IntParser DEFAULT_INT_PARSER = new IntParser() {
        public int parseInt(String value) {
            return Integer.parseInt(value);
        }

        protected Object readResolve() {
            return DEFAULT_INT_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".DEFAULT_INT_PARSER";
        }
    };

    /**
     * The default parser for float values, which are encoded by {@link Float#toString(float)}
     */
    public static final FloatParser DEFAULT_FLOAT_PARSER = new FloatParser() {
        public float parseFloat(String value) {
            return Float.parseFloat(value);
        }

        protected Object readResolve() {
            return DEFAULT_FLOAT_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".DEFAULT_FLOAT_PARSER";
        }
    };

    /**
     * The default parser for long values, which are encoded by {@link Long#toString(long)}
     */
    public static final LongParser DEFAULT_LONG_PARSER = new LongParser() {
        public long parseLong(String value) {
            return Long.parseLong(value);
        }

        protected Object readResolve() {
            return DEFAULT_LONG_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".DEFAULT_LONG_PARSER";
        }
    };

    /**
     * The default parser for double values, which are encoded by {@link Double#toString(double)}
     */
    public static final DoubleParser DEFAULT_DOUBLE_PARSER = new DoubleParser() {
        public double parseDouble(String value) {
            return Double.parseDouble(value);
        }

        protected Object readResolve() {
            return DEFAULT_DOUBLE_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".DEFAULT_DOUBLE_PARSER";
        }
    };

    /**
     * A parser instance for int values encoded by {@link NumericUtils#intToPrefixCoded(int)}, e.g. when indexed
     * via {@link NumericField}/{@link NumericTokenStream}.
     */
    public static final IntParser NUMERIC_UTILS_INT_PARSER = new IntParser() {
        public int parseInt(String val) {
            final int shift = val.charAt(0) - NumericUtils.SHIFT_START_INT;
            if (shift > 0 && shift <= 31)
                throw new FieldCacheImpl.StopFillCacheException();
            return NumericUtils.prefixCodedToInt(val);
        }

        protected Object readResolve() {
            return NUMERIC_UTILS_INT_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".NUMERIC_UTILS_INT_PARSER";
        }
    };

    /**
     * A parser instance for float values encoded with {@link NumericUtils}, e.g. when indexed
     * via {@link NumericField}/{@link NumericTokenStream}.
     */
    public static final FloatParser NUMERIC_UTILS_FLOAT_PARSER = new FloatParser() {
        public float parseFloat(String val) {
            final int shift = val.charAt(0) - NumericUtils.SHIFT_START_INT;
            if (shift > 0 && shift <= 31)
                throw new FieldCacheImpl.StopFillCacheException();
            return NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(val));
        }

        protected Object readResolve() {
            return NUMERIC_UTILS_FLOAT_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".NUMERIC_UTILS_FLOAT_PARSER";
        }
    };

    /**
     * A parser instance for long values encoded by {@link NumericUtils#longToPrefixCoded(long)}, e.g. when indexed
     * via {@link NumericField}/{@link NumericTokenStream}.
     */
    public static final LongParser NUMERIC_UTILS_LONG_PARSER = new LongParser() {
        public long parseLong(String val) {
            final int shift = val.charAt(0) - NumericUtils.SHIFT_START_LONG;
            if (shift > 0 && shift <= 63)
                throw new FieldCacheImpl.StopFillCacheException();
            return NumericUtils.prefixCodedToLong(val);
        }

        protected Object readResolve() {
            return NUMERIC_UTILS_LONG_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".NUMERIC_UTILS_LONG_PARSER";
        }
    };

    /**
     * A parser instance for double values encoded with {@link NumericUtils}, e.g. when indexed
     * via {@link NumericField}/{@link NumericTokenStream}.
     */
    public static final DoubleParser NUMERIC_UTILS_DOUBLE_PARSER = new DoubleParser() {
        public double parseDouble(String val) {
            final int shift = val.charAt(0) - NumericUtils.SHIFT_START_LONG;
            if (shift > 0 && shift <= 63)
                throw new FieldCacheImpl.StopFillCacheException();
            return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(val));
        }

        protected Object readResolve() {
            return NUMERIC_UTILS_DOUBLE_PARSER;
        }

        @Override
        public String toString() {
            return FieldCache.class.getName() + ".NUMERIC_UTILS_DOUBLE_PARSER";
        }
    };

    /**
     * Checks the internal cache for an appropriate entry, and if none is
     * found, reads the terms in <code>field</code> as a single byte and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the single byte values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public byte[] getBytes(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is found,
     * reads the terms in <code>field</code> as bytes and returns an array of
     * size <code>reader.maxDoc()</code> of the value each document has in the
     * given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the bytes.
     * @param parser Computes byte for string values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public byte[] getBytes(IndexReader reader, String field, ByteParser parser)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is
     * found, reads the terms in <code>field</code> as shorts and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the shorts.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public short[] getShorts(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is found,
     * reads the terms in <code>field</code> as shorts and returns an array of
     * size <code>reader.maxDoc()</code> of the value each document has in the
     * given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the shorts.
     * @param parser Computes short for string values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public short[] getShorts(IndexReader reader, String field, ShortParser parser)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is
     * found, reads the terms in <code>field</code> as integers and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the integers.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public int[] getInts(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is found,
     * reads the terms in <code>field</code> as integers and returns an array of
     * size <code>reader.maxDoc()</code> of the value each document has in the
     * given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the integers.
     * @param parser Computes integer for string values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public int[] getInts(IndexReader reader, String field, IntParser parser)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if
     * none is found, reads the terms in <code>field</code> as floats and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the floats.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public float[] getFloats(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if
     * none is found, reads the terms in <code>field</code> as floats and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the floats.
     * @param parser Computes float for string values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public float[] getFloats(IndexReader reader, String field,
                             FloatParser parser) throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is
     * found, reads the terms in <code>field</code> as longs and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the longs.
     * @return The values in the given field for each document.
     * @throws java.io.IOException If any error occurs.
     */
    public long[] getLongs(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is found,
     * reads the terms in <code>field</code> as longs and returns an array of
     * size <code>reader.maxDoc()</code> of the value each document has in the
     * given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the longs.
     * @param parser Computes integer for string values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public long[] getLongs(IndexReader reader, String field, LongParser parser)
            throws IOException;


    /**
     * Checks the internal cache for an appropriate entry, and if none is
     * found, reads the terms in <code>field</code> as integers and returns an array
     * of size <code>reader.maxDoc()</code> of the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the doubles.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public double[] getDoubles(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none is found,
     * reads the terms in <code>field</code> as doubles and returns an array of
     * size <code>reader.maxDoc()</code> of the value each document has in the
     * given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the doubles.
     * @param parser Computes integer for string values.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public double[] getDoubles(IndexReader reader, String field, DoubleParser parser)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none
     * is found, reads the term values in <code>field</code> and returns an array
     * of size <code>reader.maxDoc()</code> containing the value each document
     * has in the given field.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the strings.
     * @return The values in the given field for each document.
     * @throws IOException If any error occurs.
     */
    public String[] getStrings(IndexReader reader, String field)
            throws IOException;

    /**
     * Checks the internal cache for an appropriate entry, and if none
     * is found reads the term values in <code>field</code> and returns
     * an array of them in natural order, along with an array telling
     * which element in the term array each document uses.
     *
     * @param reader Used to get field values.
     * @param field  Which field contains the strings.
     * @return Array of terms and index into the array for each document.
     * @throws IOException If any error occurs.
     */
    public StringIndex getStringIndex(IndexReader reader, String field)
            throws IOException;

    /**
     * EXPERT: A unique Identifier/Description for each item in the FieldCache.
     * Can be useful for logging/debugging.
     * <p>
     * <b>EXPERIMENTAL API:</b> This API is considered extremely advanced
     * and experimental.  It may be removed or altered w/o warning in future
     * releases
     * of Lucene.
     * </p>
     */
    public static abstract class CacheEntry {
        public abstract Object getReaderKey();

        public abstract String getFieldName();

        public abstract Class getCacheType();

        public abstract Object getCustom();

        public abstract Object getValue();

        private String size = null;

        protected final void setEstimatedSize(String size) {
            this.size = size;
        }

        /**
         * @see #estimateSize(RamUsageEstimator)
         */
        public void estimateSize() {
            estimateSize(new RamUsageEstimator(false)); // doesn't check for interned
        }

        /**
         * Computes (and stores) the estimated size of the cache Value
         *
         * @see #getEstimatedSize
         */
        public void estimateSize(RamUsageEstimator ramCalc) {
            long size = ramCalc.estimateRamUsage(getValue());
            setEstimatedSize(RamUsageEstimator.humanReadableUnits
                    (size, new DecimalFormat("0.#")));

        }

        /**
         * The most recently estimated size of the value, null unless
         * estimateSize has been called.
         */
        public final String getEstimatedSize() {
            return size;
        }


        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("'").append(getReaderKey()).append("'=>");
            b.append("'").append(getFieldName()).append("',");
            b.append(getCacheType()).append(",").append(getCustom());
            b.append("=>").append(getValue().getClass().getName()).append("#");
            b.append(System.identityHashCode(getValue()));

            String s = getEstimatedSize();
            if (null != s) {
                b.append(" (size =~ ").append(s).append(')');
            }

            return b.toString();
        }

    }

    /**
     * EXPERT: Generates an array of CacheEntry objects representing all items
     * currently in the FieldCache.
     * <p>
     * NOTE: These CacheEntry objects maintain a strong reference to the
     * Cached Values.  Maintaining references to a CacheEntry the IndexReader
     * associated with it has garbage collected will prevent the Value itself
     * from being garbage collected when the Cache drops the WeakRefrence.
     * </p>
     * <p>
     * <b>EXPERIMENTAL API:</b> This API is considered extremely advanced
     * and experimental.  It may be removed or altered w/o warning in future
     * releases
     * of Lucene.
     * </p>
     */
    public abstract CacheEntry[] getCacheEntries();

    /**
     * <p>
     * EXPERT: Instructs the FieldCache to forcibly expunge all entries
     * from the underlying caches.  This is intended only to be used for
     * test methods as a way to ensure a known base state of the Cache
     * (with out needing to rely on GC to free WeakReferences).
     * It should not be relied on for "Cache maintenance" in general
     * application code.
     * </p>
     * <p>
     * <b>EXPERIMENTAL API:</b> This API is considered extremely advanced
     * and experimental.  It may be removed or altered w/o warning in future
     * releases
     * of Lucene.
     * </p>
     */
    public abstract void purgeAllCaches();

    /**
     * Expert: drops all cache entries associated with this
     * reader.  NOTE: this reader must precisely match the
     * reader that the cache entry is keyed on. If you pass a
     * top-level reader, it usually will have no effect as
     * Lucene now caches at the segment reader level.
     */
    public abstract void purge(IndexReader r);

    /**
     * If non-null, FieldCacheImpl will warn whenever
     * entries are created that are not sane according to
     * {@link org.apache.lucene.util.FieldCacheSanityChecker}.
     */
    public void setInfoStream(PrintStream stream);

    /**
     * counterpart of {@link #setInfoStream(PrintStream)}
     */
    public PrintStream getInfoStream();
}
