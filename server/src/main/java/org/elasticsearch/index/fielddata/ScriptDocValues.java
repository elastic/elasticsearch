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

package org.elasticsearch.index.fielddata;


import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.LocaleUtils;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableDuration;
import org.joda.time.ReadableInstant;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;


/**
 * Script level doc values, the assumption is that any implementation will
 * implement a <code>getValue</code> and a <code>getValues</code> that return
 * the relevant type that then can be used in scripts.
 *
 * Implementations should not internally re-use objects for the values that they
 * return as a single {@link ScriptDocValues} instance can be reused to return
 * values form multiple documents.
 */
public abstract class ScriptDocValues<T> extends AbstractList<T> {

    static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Set the current doc ID.
     */
    public abstract void setNextDocId(int docId) throws IOException;

    /**
     * Return a copy of the list of the values for the current document.
     */
    public final List<T> getValues() {
        return this;
    }

    // Throw meaningful exceptions if someone tries to modify the ScriptDocValues.
    @Override
    public final void add(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final T set(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    public static final class Longs extends ScriptDocValues<Long> {
        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger(Longs.class));

        private final SortedNumericDocValues in;
        /**
         * Callback for deprecated fields. In production this should always point to
         * {@link #deprecationLogger} but tests will override it so they can test that
         * we use the required permissions when calling it.
         */
        private final Consumer<String> deprecationCallback;
        private long[] values = new long[0];
        private int count;
        private Dates dates;
        private int docId = -1;

        /**
         * Standard constructor.
         */
        public Longs(SortedNumericDocValues in) {
            this(in, deprecationLogger::deprecated);
        }

        /**
         * Constructor for testing the deprecation callback.
         */
        Longs(SortedNumericDocValues in, Consumer<String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            this.docId = docId;
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
            if (dates != null) {
                dates.setNextDocId(docId);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public SortedNumericDocValues getInternalValues() {
            return this.in;
        }

        public long getValue() {
            if (count == 0) {
                return 0L;
            }
            return values[0];
        }

        @Deprecated
        public PainlessDateTime getDate() throws IOException {
            deprecated("getDate on numeric fields is deprecated. Use a date field to get dates.");
            if (dates == null) {
                dates = new Dates(in);
                dates.setNextDocId(docId);
            }
            return dates.getValue();
        }

        @Deprecated
        public List<PainlessDateTime> getDates() throws IOException {
            deprecated("getDates on numeric fields is deprecated. Use a date field to get dates.");
            if (dates == null) {
                dates = new Dates(in);
                dates.setNextDocId(docId);
            }
            return dates;
        }

        @Override
        public Long get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(message);
                    return null;
                }
            });
        }
    }

    public static final class Dates extends ScriptDocValues<PainlessDateTime> {
        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger(Dates.class));

        private static final PainlessDateTime EPOCH = dateTime(0);

        private final SortedNumericDocValues in;
        /**
         * Callback for deprecated fields. In production this should always point to
         * {@link #deprecationLogger} but tests will override it so they can test that
         * we use the required permissions when calling it.
         */
        private final Consumer<String> deprecationCallback;
        /**
         * Values wrapped in {@link MutableDateTime}. Null by default an allocated on first usage so we allocate a reasonably size. We keep
         * this array so we don't have allocate new {@link MutableDateTime}s on every usage. Instead we reuse them for every document.
         */
        private PainlessDateTime[] dates;
        private int count;

        /**
         * Standard constructor.
         */
        public Dates(SortedNumericDocValues in) {
            this(in, deprecationLogger::deprecated);
        }

        /**
         * Constructor for testing deprecation logging.
         */
        Dates(SortedNumericDocValues in, Consumer<String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public PainlessDateTime getValue() {
            if (count == 0) {
                return EPOCH;
            }
            return get(0);
        }

        /**
         * Fetch the first value. Added for backwards compatibility with 5.x when date fields were {@link Longs}.
         */
        @Deprecated
        public PainlessDateTime getDate() {
            deprecated("getDate is no longer necessary on date fields as the value is now a date.");
            return getValue();
        }

        /**
         * Fetch all the values. Added for backwards compatibility with 5.x when date fields were {@link Longs}.
         */
        @Deprecated
        public List<PainlessDateTime> getDates() {
            deprecated("getDates is no longer necessary on date fields as the values are now dates.");
            return this;
        }

        @Override
        public PainlessDateTime get(int index) {
            if (index >= count) {
                throw new IndexOutOfBoundsException(
                        "attempted to fetch the [" + index + "] date when there are only ["
                                + count + "] dates.");
            }
            return dates[index];
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                count = in.docValueCount();
            } else {
                count = 0;
            }
            refreshArray();
        }

        /**
         * Refresh the backing array. Package private so it can be called when {@link Longs} loads dates.
         */
        void refreshArray() throws IOException {
            if (count == 0) {
                return;
            }
            if (dates == null) {
                // Happens for the document. We delay allocating dates so we can allocate it with a reasonable size.
                dates = new PainlessDateTime[count];
                for (int i = 0; i < dates.length; i++) {
                    dates[i] = dateTime();
                }
                return;
            }
            if (count > dates.length) {
                // Happens when we move to a new document and it has more dates than any documents before it.
                PainlessDateTime[] backup = dates;
                dates = new PainlessDateTime[count];
                System.arraycopy(backup, 0, dates, 0, backup.length);
                for (int i = 0; i < backup.length; i++) {
                    dates[i].setMillis(in.nextValue());
                }
                for (int i = backup.length; i < dates.length; i++) {
                    dates[i] = dateTime();
                }
                return;
            }
            for (int i = 0; i < count; i++) {
                dates[i] = dateTime();
            }
        }

        private PainlessDateTime dateTime() throws IOException {
            return dateTime(in.nextValue());
        }

        private static PainlessDateTime dateTime(final long value) {
            return new PainlessDateTime(new MutableDateTime(value, DateTimeZone.UTC));
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(message);
                    return null;
                }
            });
        }
    }

    /**
     * Wrap {@link MutableDateTime} so we can easily overload the "properties"
     * with parameters and add new getters such as getDayName.
     */
    public static class PainlessDateTime extends MutableDateTime {
        private final MutableDateTime target;

        PainlessDateTime(MutableDateTime target) {
            this.target = target;
        }

        // "extension methods" for MutableDateTime which support overloaded forms
        // of all property getters...

        public String getDayName() {
            return this.getDayName(this.target, Locale.getDefault());
        }

        public String getDayName(final String timeZoneId) {
            return getDayName(this.setTimeZoneId(timeZoneId), Locale.getDefault());
        }

        public String getDayName(final String timeZoneId, final String locale) {
            return getDayName(this.setTimeZoneId(timeZoneId), locale(locale));
        }

        private ReadableDateTime setTimeZoneId(final String timeZoneId) {
            final TimeZone timeZone = TimeZone.getTimeZone(timeZoneId);
            return UTC.equals(timeZone) ?
                this.target :
                this.target.toDateTime()
                    .withZone(DateTimeZone.forTimeZone(timeZone));
        }

        private Locale locale(final String locale) {
            return LocaleUtils.parse(locale);
        }

        private String getDayName(final ReadableDateTime dateTime, Locale locale) {
            final java.time.Instant instant = java.time.Instant.ofEpochMilli(dateTime.getMillis());
            final ZoneId zoneId = ZoneId.of(dateTime.getZone().getID(), ZoneId.SHORT_IDS);
            final ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);

            // DayName.FORMAT
            return zdt.format(java.time.format.DateTimeFormatter.ofPattern("EEEE", locale)); // long form name of week.
        }

        // delegate all public methods to target...

        @Override
        public DateTimeField getRoundingField() {
            return target.getRoundingField();
        }

        @Override
        public int getRoundingMode() {
            return target.getRoundingMode();
        }

        @Override
        public void setRounding(DateTimeField field) {
            target.setRounding(field);
        }

        @Override
        public void setRounding(DateTimeField field, int mode) {
            target.setRounding(field, mode);
        }

        @Override
        public void setMillis(long instant) {
            target.setMillis(instant);
        }

        @Override
        public void setMillis(ReadableInstant instant) {
            target.setMillis(instant);
        }

        @Override
        public void add(long duration) {
            target.add(duration);
        }

        @Override
        public void add(ReadableDuration duration) {
            target.add(duration);
        }

        @Override
        public void add(ReadableDuration duration, int scalar) {
            target.add(duration, scalar);
        }

        @Override
        public void add(ReadablePeriod period) {
            target.add(period);
        }

        @Override
        public void add(ReadablePeriod period, int scalar) {
            target.add(period, scalar);
        }

        @Override
        public void setChronology(Chronology chronology) {
            target.setChronology(chronology);
        }

        @Override
        public void setZone(DateTimeZone newZone) {
            target.setZone(newZone);
        }

        @Override
        public void setZoneRetainFields(DateTimeZone newZone) {
            target.setZoneRetainFields(newZone);
        }

        @Override
        public void set(DateTimeFieldType type, int value) {
            target.set(type, value);
        }

        @Override
        public void add(DurationFieldType type, int amount) {
            target.add(type, amount);
        }

        @Override
        public void setYear(int year) {
            target.setYear(year);
        }

        @Override
        public void addYears(int years) {
            target.addYears(years);
        }

        @Override
        public void setWeekyear(int weekyear) {
            target.setWeekyear(weekyear);
        }

        @Override
        public void addWeekyears(int weekyears) {
            target.addWeekyears(weekyears);
        }

        @Override
        public void setMonthOfYear(int monthOfYear) {
            target.setMonthOfYear(monthOfYear);
        }

        @Override
        public void addMonths(int months) {
            target.addMonths(months);
        }

        @Override
        public void setWeekOfWeekyear(int weekOfWeekyear) {
            target.setWeekOfWeekyear(weekOfWeekyear);
        }

        @Override
        public void addWeeks(int weeks) {
            target.addWeeks(weeks);
        }

        @Override
        public void setDayOfYear(int dayOfYear) {
            target.setDayOfYear(dayOfYear);
        }

        @Override
        public void setDayOfMonth(int dayOfMonth) {
            target.setDayOfMonth(dayOfMonth);
        }

        @Override
        public void setDayOfWeek(int dayOfWeek) {
            target.setDayOfWeek(dayOfWeek);
        }

        @Override
        public void addDays(int days) {
            target.addDays(days);
        }

        @Override
        public void setHourOfDay(int hourOfDay) {
            target.setHourOfDay(hourOfDay);
        }

        @Override
        public void addHours(int hours) {
            target.addHours(hours);
        }

        @Override
        public void setMinuteOfDay(int minuteOfDay) {
            target.setMinuteOfDay(minuteOfDay);
        }

        @Override
        public void setMinuteOfHour(int minuteOfHour) {
            target.setMinuteOfHour(minuteOfHour);
        }

        @Override
        public void addMinutes(int minutes) {
            target.addMinutes(minutes);
        }

        @Override
        public void setSecondOfDay(int secondOfDay) {
            target.setSecondOfDay(secondOfDay);
        }

        @Override
        public void setSecondOfMinute(int secondOfMinute) {
            target.setSecondOfMinute(secondOfMinute);
        }

        @Override
        public void addSeconds(int seconds) {
            target.addSeconds(seconds);
        }

        @Override
        public void setMillisOfDay(int millisOfDay) {
            target.setMillisOfDay(millisOfDay);
        }

        @Override
        public void setMillisOfSecond(int millisOfSecond) {
            target.setMillisOfSecond(millisOfSecond);
        }

        @Override
        public void addMillis(int millis) {
            target.addMillis(millis);
        }

        @Override
        public void setDate(long instant) {
            target.setDate(instant);
        }

        @Override
        public void setDate(ReadableInstant instant) {
            target.setDate(instant);
        }

        @Override
        public void setDate(int year, int monthOfYear, int dayOfMonth) {
            target.setDate(year, monthOfYear, dayOfMonth);
        }

        @Override
        public void setTime(long millis) {
            target.setTime(millis);
        }

        @Override
        public void setTime(ReadableInstant instant) {
            target.setTime(instant);
        }

        @Override
        public void setTime(int hour, int minuteOfHour, int secondOfMinute, int millisOfSecond) {
            target.setTime(hour, minuteOfHour, secondOfMinute, millisOfSecond);
        }

        @Override
        public void setDateTime(int year,
                                int monthOfYear,
                                int dayOfMonth,
                                int hourOfDay,
                                int minuteOfHour,
                                int secondOfMinute,
                                int millisOfSecond) {
            target.setDateTime(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond);
        }

        @Override
        public Property property(DateTimeFieldType type) {
            return target.property(type);
        }

        @Override
        public Property era() {
            return target.era();
        }

        @Override
        public Property centuryOfEra() {
            return target.centuryOfEra();
        }

        @Override
        public Property yearOfCentury() {
            return target.yearOfCentury();
        }

        @Override
        public Property yearOfEra() {
            return target.yearOfEra();
        }

        @Override
        public Property year() {
            return target.year();
        }

        @Override
        public Property weekyear() {
            return target.weekyear();
        }

        @Override
        public Property monthOfYear() {
            return target.monthOfYear();
        }

        @Override
        public Property weekOfWeekyear() {
            return target.weekOfWeekyear();
        }

        @Override
        public Property dayOfYear() {
            return target.dayOfYear();
        }

        @Override
        public Property dayOfMonth() {
            return target.dayOfMonth();
        }

        @Override
        public Property dayOfWeek() {
            return target.dayOfWeek();
        }

        @Override
        public Property hourOfDay() {
            return target.hourOfDay();
        }

        @Override
        public Property minuteOfDay() {
            return target.minuteOfDay();
        }

        @Override
        public Property minuteOfHour() {
            return target.minuteOfHour();
        }

        @Override
        public Property secondOfDay() {
            return target.secondOfDay();
        }

        @Override
        public Property secondOfMinute() {
            return target.secondOfMinute();
        }

        @Override
        public Property millisOfDay() {
            return target.millisOfDay();
        }

        @Override
        public Property millisOfSecond() {
            return target.millisOfSecond();
        }

        @Override
        public MutableDateTime copy() {
            return new PainlessDateTime(target.copy());
        }

        @Override
        public Object clone() {
            return new PainlessDateTime((MutableDateTime) target.clone());
        }

        @Override
        public long getMillis() {
            return target.getMillis();
        }

        @Override
        public Chronology getChronology() {
            return target.getChronology();
        }

        @Override
        public int get(DateTimeFieldType type) {
            return target.get(type);
        }

        @Override
        public int getEra() {
            return target.getEra();
        }

        @Override
        public int getCenturyOfEra() {
            return target.getCenturyOfEra();
        }

        @Override
        public int getYearOfEra() {
            return target.getYearOfEra();
        }

        @Override
        public int getYearOfCentury() {
            return target.getYearOfCentury();
        }

        @Override
        public int getYear() {
            return target.getYear();
        }

        public int getYear(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getYear();
        }

        @Override
        public int getWeekyear() {
            return target.getWeekyear();
        }

        @Override
        public int getMonthOfYear() {
            return target.getMonthOfYear();
        }

        public int getMonthOfYear(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getMonthOfYear();
        }

        @Override
        public int getWeekOfWeekyear() {
            return target.getWeekOfWeekyear();
        }

        public int getWeekOfWeekyear(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getWeekOfWeekyear();
        }

        @Override
        public int getDayOfYear() {
            return target.getDayOfYear();
        }

        public int getDayOfYear(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getDayOfYear();
        }

        @Override
        public int getDayOfMonth() {
            return target.getDayOfMonth();
        }

        public int getDayOfMonth(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getDayOfMonth();
        }

        @Override
        public int getDayOfWeek() {
            return target.getDayOfWeek();
        }

        public int getDayOfWeek(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getDayOfWeek();
        }

        @Override
        public int getHourOfDay() {
            return target.getHourOfDay();
        }

        public int getHourOfDay(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getHourOfDay();
        }

        @Override
        public int getMinuteOfDay() {
            return target.getMinuteOfDay();
        }

        public int getMinuteOfDay(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getMinuteOfDay();
        }

        @Override
        public int getMinuteOfHour() {
            return target.getMinuteOfHour();
        }

        public int getMinuteOfHour(final String timeZoneId) {
            return this.setTimeZoneId(timeZoneId).getMinuteOfHour();
        }

        @Override
        public int getSecondOfDay() {
            return target.getSecondOfDay();
        }

        @Override
        public int getSecondOfMinute() {
            return target.getSecondOfMinute();
        }

        @Override
        public int getMillisOfDay() {
            return target.getMillisOfDay();
        }

        @Override
        public int getMillisOfSecond() {
            return target.getMillisOfSecond();
        }

        @Override
        public Calendar toCalendar(Locale locale) {
            return target.toCalendar(locale);
        }

        @Override
        public GregorianCalendar toGregorianCalendar() {
            return target.toGregorianCalendar();
        }

        @Override
        public String toString() {
            return target.toString();
        }

        @Override
        public String toString(String pattern) {
            return target.toString(pattern);
        }

        @Override
        public String toString(String pattern, Locale locale) throws IllegalArgumentException {
            return target.toString(pattern, locale);
        }

        @Override
        public DateTimeZone getZone() {
            return target.getZone();
        }

        @Override
        public boolean isSupported(DateTimeFieldType type) {
            return target.isSupported(type);
        }

        @Override
        public int get(DateTimeField field) {
            return target.get(field);
        }

        @Override
        public Instant toInstant() {
            return target.toInstant();
        }

        @Override
        public DateTime toDateTime() {
            return target.toDateTime();
        }

        @Override
        public DateTime toDateTimeISO() {
            return target.toDateTimeISO();
        }

        @Override
        public DateTime toDateTime(DateTimeZone zone) {
            return target.toDateTime(zone);
        }

        @Override
        public DateTime toDateTime(Chronology chronology) {
            return target.toDateTime(chronology);
        }

        @Override
        public MutableDateTime toMutableDateTime() {
            return target.toMutableDateTime();
        }

        @Override
        public MutableDateTime toMutableDateTimeISO() {
            return target.toMutableDateTimeISO();
        }

        @Override
        public MutableDateTime toMutableDateTime(DateTimeZone zone) {
            return target.toMutableDateTime(zone);
        }

        @Override
        public MutableDateTime toMutableDateTime(Chronology chronology) {
            return target.toMutableDateTime(chronology);
        }

        @Override
        public Date toDate() {
            return target.toDate();
        }

        @Override
        public boolean equals(Object readableInstant) {
            return this == readableInstant || target.equals(readableInstant);
        }

        @Override
        public int hashCode() {
            return target.hashCode();
        }

        @Override
        public int compareTo(ReadableInstant other) {
            return target.compareTo(other);
        }

        @Override
        public boolean isAfter(long instant) {
            return target.isAfter(instant);
        }

        @Override
        public boolean isAfterNow() {
            return target.isAfterNow();
        }

        @Override
        public boolean isAfter(ReadableInstant instant) {
            return target.isAfter(instant);
        }

        @Override
        public boolean isBefore(long instant) {
            return target.isBefore(instant);
        }

        @Override
        public boolean isBeforeNow() {
            return target.isBeforeNow();
        }

        @Override
        public boolean isBefore(ReadableInstant instant) {
            return target.isBefore(instant);
        }

        @Override
        public boolean isEqual(long instant) {
            return target.isEqual(instant);
        }

        @Override
        public boolean isEqualNow() {
            return target.isEqualNow();
        }

        @Override
        public boolean isEqual(ReadableInstant instant) {
            return target.isEqual(instant);
        }

        @Override
        public String toString(DateTimeFormatter formatter) {
            return target.toString(formatter);
        }
    }

    public static final class Doubles extends ScriptDocValues<Double> {

        private final SortedNumericDoubleValues in;
        private double[] values = new double[0];
        private int count;

        public Doubles(SortedNumericDoubleValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public SortedNumericDoubleValues getInternalValues() {
            return this.in;
        }

        public double getValue() {
            if (count == 0) {
                return 0d;
            }
            return values[0];
        }

        @Override
        public Double get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    public static final class GeoPoints extends ScriptDocValues<GeoPoint> {

        private final MultiGeoPointValues in;
        private GeoPoint[] values = new GeoPoint[0];
        private int count;

        public GeoPoints(MultiGeoPointValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    GeoPoint point = in.nextValue();
                    values[i] = new GeoPoint(point.lat(), point.lon());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                values[i] = new GeoPoint();
                }
            }
        }

        public GeoPoint getValue() {
            if (count == 0) {
                return null;
            }
            return values[0];
        }

        public double getLat() {
            return getValue().lat();
        }

        public double[] getLats() {
            List<GeoPoint> points = getValues();
            double[] lats = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lats[i] = points.get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            List<GeoPoint> points = getValues();
            double[] lons = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lons[i] = points.get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public GeoPoint get(int index) {
            final GeoPoint point = values[index];
            return new GeoPoint(point.lat(), point.lon());
        }

        @Override
        public int size() {
            return count;
        }

        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), lat, lon);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return arcDistance(lat, lon);
        }

        public double planeDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.planeDistance(point.lat(), point.lon(), lat, lon);
        }

        public double planeDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return planeDistance(lat, lon);
        }

        public double geohashDistance(String geohash) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), GeoHashUtils.decodeLatitude(geohash),
                GeoHashUtils.decodeLongitude(geohash));
        }

        public double geohashDistanceWithDefault(String geohash, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return geohashDistance(geohash);
        }
    }

    public static final class Booleans extends ScriptDocValues<Boolean> {

        private final SortedNumericDocValues in;
        private boolean[] values = new boolean[0];
        private int count;

        public Booleans(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue() == 1;
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = grow(values, count);
        }

        public boolean getValue() {
            return count != 0 && values[0];
        }

        @Override
        public Boolean get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        private static boolean[] grow(boolean[] array, int minSize) {
            assert minSize >= 0 : "size must be positive (got " + minSize
                    + "): likely integer overflow?";
            if (array.length < minSize) {
                return Arrays.copyOf(array, ArrayUtil.oversize(minSize, 1));
            } else
                return array;
        }

    }

    abstract static class BinaryScriptDocValues<T> extends ScriptDocValues<T> {

        private final SortedBinaryDocValues in;
        protected BytesRefBuilder[] values = new BytesRefBuilder[0];
        protected int count;

        BinaryScriptDocValues(SortedBinaryDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    // We need to make a copy here, because BytesBinaryDVAtomicFieldData's SortedBinaryDocValues
                    // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                    // instance for all slots in the values array.
                    values[i].copyBytes(in.nextValue());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                final int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        @Override
        public int size() {
            return count;
        }

    }

    public static final class Strings extends BinaryScriptDocValues<String> {

        public Strings(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public String get(int index) {
            return values[index].get().utf8ToString();
        }

        public String getValue() {
            return count == 0 ? null : get(0);
        }
    }

    public static final class BytesRefs extends BinaryScriptDocValues<BytesRef> {

        public BytesRefs(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public BytesRef get(int index) {
            /**
             * We need to make a copy here because {@link BinaryScriptDocValues} might reuse the
             * returned value and the same instance might be used to
             * return values from multiple documents.
             **/
            return values[index].toBytesRef();
        }

        public BytesRef getValue() {
            return count == 0 ? new BytesRef() : get(0);
        }

    }
}
