/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.longEncode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DocValueFormatTests extends ESTestCase {

    public void testSerialization() throws Exception {
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::readFrom));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::readFrom));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.GEOTILE.getWriteableName(), in -> DocValueFormat.GEOTILE));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.BINARY.getWriteableName(), in -> DocValueFormat.BINARY));
        NamedWriteableRegistry registry = new NamedWriteableRegistry(entries);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.BOOLEAN);
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.BOOLEAN, in.readNamedWriteable(DocValueFormat.class));

        DocValueFormat.Decimal decimalFormat = new DocValueFormat.Decimal("###.##");
        out = new BytesStreamOutput();
        out.writeNamedWriteable(decimalFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        DocValueFormat vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.Decimal.class, vf.getClass());
        assertEquals("###.##", ((DocValueFormat.Decimal) vf).pattern);

        DateFormatter formatter = DateFormatter.forPattern("epoch_second");
        DocValueFormat.DateTime dateFormat = new DocValueFormat.DateTime(formatter, ZoneOffset.ofHours(1), Resolution.MILLISECONDS);
        out = new BytesStreamOutput();
        out.writeNamedWriteable(dateFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.pattern());
        assertEquals(ZoneOffset.ofHours(1), ((DocValueFormat.DateTime) vf).timeZone);
        assertEquals(Resolution.MILLISECONDS, ((DocValueFormat.DateTime) vf).resolution);

        dateFormat = (DocValueFormat.DateTime) DocValueFormat.enableFormatSortValues(dateFormat);
        out = new BytesStreamOutput();
        out.writeNamedWriteable(dateFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.pattern());
        assertEquals(ZoneOffset.ofHours(1), ((DocValueFormat.DateTime) vf).timeZone);
        assertEquals(Resolution.MILLISECONDS, ((DocValueFormat.DateTime) vf).resolution);
        assertTrue(dateFormat.formatSortValues);

        DocValueFormat.DateTime nanosDateFormat = new DocValueFormat.DateTime(formatter, ZoneOffset.ofHours(1), Resolution.NANOSECONDS);
        out = new BytesStreamOutput();
        out.writeNamedWriteable(nanosDateFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.pattern());
        assertEquals(ZoneOffset.ofHours(1), ((DocValueFormat.DateTime) vf).timeZone);
        assertEquals(Resolution.NANOSECONDS, ((DocValueFormat.DateTime) vf).resolution);

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.GEOHASH);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.GEOHASH, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.GEOTILE);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.GEOTILE, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.IP);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.IP, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.RAW);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.RAW, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.BINARY);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.BINARY, in.readNamedWriteable(DocValueFormat.class));
    }

    public void testRawFormat() {
        assertEquals(0L, DocValueFormat.RAW.format(0));
        assertEquals(-1L, DocValueFormat.RAW.format(-1));
        assertEquals(1L, DocValueFormat.RAW.format(1));

        assertEquals(0d, DocValueFormat.RAW.format(0d));
        assertEquals(.5d, DocValueFormat.RAW.format(.5d));
        assertEquals(-1d, DocValueFormat.RAW.format(-1d));

        assertEquals("abc", DocValueFormat.RAW.format(new BytesRef("abc")));
    }

    public void testBinaryFormat() {
        assertEquals("", DocValueFormat.BINARY.format(new BytesRef()));
        assertEquals("KmQ=", DocValueFormat.BINARY.format(new BytesRef(new byte[] { 42, 100 })));

        assertEquals(new BytesRef(), DocValueFormat.BINARY.parseBytesRef(""));
        assertEquals(new BytesRef(new byte[] { 42, 100 }), DocValueFormat.BINARY.parseBytesRef("KmQ="));
    }

    public void testBooleanFormat() {
        assertEquals(false, DocValueFormat.BOOLEAN.format(0));
        assertEquals(true, DocValueFormat.BOOLEAN.format(1));
    }

    public void testIpFormat() {
        assertEquals(
            "192.168.1.7",
            DocValueFormat.IP.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.7"))))
        );
        assertEquals("::1", DocValueFormat.IP.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1")))));
    }

    public void testDecimalFormat() {
        DocValueFormat formatter = new DocValueFormat.Decimal("###.##");
        assertEquals("0", formatter.format(0.0d));
        assertEquals("1", formatter.format(1d));
        formatter = new DocValueFormat.Decimal("000.000");
        assertEquals("-000.500", formatter.format(-0.5));
        formatter = new DocValueFormat.Decimal("###,###.###");
        assertEquals("0.86", formatter.format(0.8598023539251286d));
        formatter = new DocValueFormat.Decimal("###,###.###");
        assertEquals("859,802.354", formatter.format(0.8598023539251286d * 1_000_000));
    }

    public void testGeoTileFormat() {
        assertEquals("0/0/0", DocValueFormat.GEOTILE.format(longEncode(0, 0, 0)));
        assertEquals("15/19114/7333", DocValueFormat.GEOTILE.format(longEncode(30, 70, 15)));
        assertEquals("29/536869420/0", DocValueFormat.GEOTILE.format(longEncode(179.999, 89.999, 29)));
        assertEquals("29/1491/536870911", DocValueFormat.GEOTILE.format(longEncode(-179.999, -89.999, 29)));
        assertEquals("2/2/1", DocValueFormat.GEOTILE.format(longEncode(1, 1, 2)));
        assertEquals("1/1/0", DocValueFormat.GEOTILE.format(longEncode(13, GeoUtils.normalizeLat(95), 1)));
        assertEquals("1/1/1", DocValueFormat.GEOTILE.format(longEncode(13, GeoUtils.normalizeLat(-95), 1)));
    }

    public void testRawParse() {
        assertEquals(-1L, DocValueFormat.RAW.parseLong("-1", randomBoolean(), null));
        assertEquals(1L, DocValueFormat.RAW.parseLong("1", randomBoolean(), null));
        assertEquals(Long.MAX_VALUE - 2, DocValueFormat.RAW.parseLong(Long.toString(Long.MAX_VALUE - 2), randomBoolean(), null));
        // not checking exception messages as they could depend on the JVM
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("", randomBoolean(), null));
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("abc", randomBoolean(), null));

        assertEquals(-1d, DocValueFormat.RAW.parseDouble("-1", randomBoolean(), null), 0d);
        assertEquals(1d, DocValueFormat.RAW.parseDouble("1", randomBoolean(), null), 0d);
        assertEquals(.5, DocValueFormat.RAW.parseDouble("0.5", randomBoolean(), null), 0d);
        // not checking exception messages as they could depend on the JVM
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseDouble("", randomBoolean(), null));
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseDouble("abc", randomBoolean(), null));

        assertEquals(new BytesRef("abc"), DocValueFormat.RAW.parseBytesRef("abc"));
    }

    public void testBooleanParse() {
        assertEquals(0L, DocValueFormat.BOOLEAN.parseLong("false", randomBoolean(), null));
        assertEquals(1L, DocValueFormat.BOOLEAN.parseLong("true", randomBoolean(), null));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DocValueFormat.BOOLEAN.parseLong("", randomBoolean(), null)
        );
        assertEquals("Cannot parse boolean [], expected either [true] or [false]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> DocValueFormat.BOOLEAN.parseLong("0", randomBoolean(), null));
        assertEquals("Cannot parse boolean [0], expected either [true] or [false]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> DocValueFormat.BOOLEAN.parseLong("False", randomBoolean(), null));
        assertEquals("Cannot parse boolean [False], expected either [true] or [false]", e.getMessage());
    }

    public void testIPParse() {
        assertEquals(
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.7"))),
            DocValueFormat.IP.parseBytesRef("192.168.1.7")
        );
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), DocValueFormat.IP.parseBytesRef("::1"));
    }

    public void testDecimalParse() {
        DocValueFormat parser = new DocValueFormat.Decimal("###.##");
        assertEquals(0.0d, parser.parseDouble(randomFrom("0.0", "0", ".0", ".0000"), true, null), 0.0d);
        assertEquals(-1.0d, parser.parseDouble(randomFrom("-1.0", "-1", "-1.0", "-1.0000"), true, null), 0.0d);
        assertEquals(0.0d, parser.parseLong("0", true, null), 0.0d);
        assertEquals(1.0d, parser.parseLong("1", true, null), 0.0d);
        parser = new DocValueFormat.Decimal("###,###.###");
        assertEquals(859802.354d, parser.parseDouble("859,802.354", true, null), 0.0d);
        assertEquals(0.859d, parser.parseDouble("0.859", true, null), 0.0d);
        assertEquals(0.8598023539251286d, parser.parseDouble("0.8598023539251286", true, null), 0.0d);
    }

    public void testFormatSortFieldOutput() {
        DateFormatter formatter = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss");
        DocValueFormat.DateTime dateFormat = new DocValueFormat.DateTime(formatter, ZoneOffset.ofHours(1), Resolution.MILLISECONDS);
        assertThat(dateFormat.formatSortValue(1415580798601L), equalTo(1415580798601L));
        dateFormat = (DocValueFormat.DateTime) DocValueFormat.enableFormatSortValues(dateFormat);
        assertThat(dateFormat.formatSortValue(1415580798601L), equalTo("2014-11-10 01:53:18"));
    }

    public void testBadUtf8() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DocValueFormat.RAW.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("0.0.0.0"))))
        );
        assertNotNull("wrapped exception should have a cause", e.getCause());
        assertThat(e.getMessage(), containsString("mapping"));
        assertThat(e.getMessage(), containsString("UTF8"));
    }

    public void testBadIp() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DocValueFormat.IP.format(new BytesRef("cat")));
        assertNotNull("wrapped exception should have a cause", e.getCause());
        assertThat(e.getMessage(), containsString("mapping"));
        assertThat(e.getMessage(), containsString("IP address"));
    }

    /**
     * <p>Test that if we format a datetime using the `epoch_second` format, we can then parse the result
     * back into the same value we started with, for all timezones</p>
     *
     * <p>"Why would you put a timezone on epoch_seconds? it doesn't make sense" you might be asking.
     * I was.  The key to remember here is that we use the same time zone parameter for date histogram
     * bucket generation and formatting.  So, while asking for (e.g.) epoch_seconds in New York time
     * is nonsensical, asking for day-buckets in New York time with the keys formatted in epoch_seconds
     * is pretty standard.</p>
     *
     * <p>This test validates that if someone does this in composite, we can then parse the after key
     * we generated into the correct value.  Parsing also happens on missing values.</p>
     */
    public void testParseEpochSecondsTimezone() {
        ZoneId zone = randomZone();
        DocValueFormat.DateTime formatter = new DocValueFormat.DateTime(
            DateFormatter.forPattern("epoch_second"),
            zone,
            Resolution.MILLISECONDS
        );
        long millis = randomLong();
        // Convert to seconds
        millis -= (millis % 1000);
        assertEquals("failed formatting for tz " + zone, millis, formatter.parseLong(formatter.format(millis), false, () -> {
            throw new UnsupportedOperationException("don't use now");
        }));
    }

    public void testParseEpochMillisTimezone() {
        ZoneId zone = randomZone();
        DocValueFormat.DateTime formatter = new DocValueFormat.DateTime(
            DateFormatter.forPattern("epoch_millis"),
            zone,
            Resolution.MILLISECONDS
        );
        long millis = randomLong();
        assertEquals("failed formatting for tz " + zone, millis, formatter.parseLong(formatter.format(millis), false, () -> {
            throw new UnsupportedOperationException("don't use now");
        }));
    }

    public void testDateHMSTimezone() {
        DocValueFormat.DateTime tokyo = new DocValueFormat.DateTime(
            DateFormatter.forPattern("date_hour_minute_second"),
            ZoneOffset.ofHours(9),
            Resolution.MILLISECONDS
        );
        DocValueFormat.DateTime utc = new DocValueFormat.DateTime(
            DateFormatter.forPattern("date_hour_minute_second"),
            ZoneOffset.UTC,
            Resolution.MILLISECONDS
        );
        long millis = 1622567918000L;
        assertEquals("2021-06-01T17:18:38", utc.format(millis));
        assertEquals("2021-06-02T02:18:38", tokyo.format(millis));
        assertEquals("couldn't parse UTC", millis, utc.parseLong(utc.format(millis), false, () -> {
            throw new UnsupportedOperationException("don't use now");
        }));
        assertEquals("couldn't parse Tokyo", millis, tokyo.parseLong(tokyo.format(millis), false, () -> {
            throw new UnsupportedOperationException("don't use now");
        }));
    }

    public void testDateTimeWithTimezone() {

        DocValueFormat.DateTime tokyo = new DocValueFormat.DateTime(
            DateFormatter.forPattern("basic_date_time_no_millis"),
            ZoneOffset.ofHours(9),
            Resolution.MILLISECONDS
        );
        DocValueFormat.DateTime utc = new DocValueFormat.DateTime(
            DateFormatter.forPattern("basic_date_time_no_millis"),
            ZoneOffset.UTC,
            Resolution.MILLISECONDS
        );
        long millis = 1622567918000L;
        assertEquals("20210601T171838Z", utc.format(millis));
        assertEquals("20210602T021838+09:00", tokyo.format(millis));
        assertEquals("couldn't parse UTC", millis, utc.parseLong(utc.format(millis), false, () -> {
            throw new UnsupportedOperationException("don't use now");
        }));
        assertEquals("couldn't parse Tokyo", millis, tokyo.parseLong(tokyo.format(millis), false, () -> {
            throw new UnsupportedOperationException("don't use now");
        }));
    }

    /**
     * This is a regression test for https://github.com/elastic/elasticsearch/issues/76415
     */
    public void testParseOffset() {
        DocValueFormat.DateTime parsesZone = new DocValueFormat.DateTime(
            DateFormatter.forPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSSZZZZZ"),
            ZoneOffset.UTC,
            Resolution.MILLISECONDS
        );
        long expected = 1628719200000L;
        ZonedDateTime sample = ZonedDateTime.of(2021, 8, 12, 0, 0, 0, 0, ZoneId.ofOffset("", ZoneOffset.ofHours(2)));
        assertEquals("GUARD: wrong initial millis", expected, sample.toEpochSecond() * 1000);
        long actualMillis = parsesZone.parseLong("2021-08-12T00:00:00.000000000+02:00", false, () -> {
            throw new UnsupportedOperationException("don't use now");
        });
        assertEquals(expected, actualMillis);
    }

    /**
     * Make sure fixing 76415 doesn't break parsing zone strings
     */
    public void testParseZone() {
        DocValueFormat.DateTime parsesZone = new DocValueFormat.DateTime(
            DateFormatter.forPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSSVV"),
            ZoneOffset.UTC,
            Resolution.MILLISECONDS
        );
        long expected = 1628719200000L;
        ZonedDateTime sample = ZonedDateTime.of(2021, 8, 12, 0, 0, 0, 0, ZoneId.ofOffset("", ZoneOffset.ofHours(2)));
        assertEquals("GUARD: wrong initial millis", expected, sample.toEpochSecond() * 1000);
        // assertEquals("GUARD: wrong initial string", "2021-08-12T00:00:00.000000000+02:00", parsesZone.format(expected));
        long actualMillis = parsesZone.parseLong("2021-08-12T00:00:00.000000000CET", false, () -> {
            throw new UnsupportedOperationException("don't use now");
        });
        assertEquals(expected, actualMillis);
    }

    public void testParseTsid() throws IOException {
        var routingFields = new RoutingPathFields(null);
        routingFields.addString("string", randomAlphaOfLength(10));
        routingFields.addLong("long", randomLong());
        routingFields.addUnsignedLong("ulong", randomLong());
        BytesRef expected = routingFields.buildHash().toBytesRef();
        byte[] expectedBytes = new byte[expected.length];
        System.arraycopy(expected.bytes, 0, expectedBytes, 0, expected.length);
        BytesRef actual = DocValueFormat.TIME_SERIES_ID.parseBytesRef(expected);
        assertEquals(expected, actual);
        Object tsidFormat = DocValueFormat.TIME_SERIES_ID.format(expected);
        Object tsidBase64 = Base64.getUrlEncoder().withoutPadding().encodeToString(expectedBytes);
        assertEquals(tsidFormat, tsidBase64);
    }

    public void testFormatAndParseTsRoutingHash() throws IOException {
        BytesRef tsRoutingHashInput = new BytesRef("cn4exQ");
        DocValueFormat docValueFormat = TimeSeriesRoutingHashFieldMapper.INSTANCE.fieldType().docValueFormat(null, ZoneOffset.UTC);
        Object formattedValue = docValueFormat.format(tsRoutingHashInput);
        // the format method takes BytesRef as input and outputs a String
        assertThat(formattedValue, instanceOf(String.class));
        // the parse method will output the BytesRef input
        assertThat(docValueFormat.parseBytesRef(formattedValue), is(tsRoutingHashInput));
    }
}
