/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TopNEncoderTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { TopNEncoder.DEFAULT_SORTABLE },
            new Object[] { TopNEncoder.UTF8 },
            new Object[] { TopNEncoder.VERSION },
            new Object[] { TopNEncoder.IP },
            new Object[] { TopNEncoder.DEFAULT_UNSORTABLE }
        );
    }

    private final TopNEncoder encoder;

    public TopNEncoderTests(TopNEncoder encoder) {
        this.encoder = encoder;
    }

    public void testLong() {
        BreakingBytesRefBuilder builder = ExtractorTests.nonBreakingBytesRefBuilder();
        long v = randomLong();
        encoder.encodeLong(v, builder);
        BytesRef encoded = builder.bytesRefView();
        assertThat(encoder.decodeLong(encoded), equalTo(v));
        assertThat(encoded.length, equalTo(0));
    }

    public void testInt() {
        BreakingBytesRefBuilder builder = ExtractorTests.nonBreakingBytesRefBuilder();
        int v = randomInt();
        encoder.encodeInt(v, builder);
        BytesRef encoded = builder.bytesRefView();
        assertThat(encoder.decodeInt(encoded), equalTo(v));
        assertThat(encoded.length, equalTo(0));
    }

    public void testDouble() {
        BreakingBytesRefBuilder builder = ExtractorTests.nonBreakingBytesRefBuilder();
        double v = randomDouble();
        encoder.encodeDouble(v, builder);
        BytesRef encoded = builder.bytesRefView();
        assertThat(encoder.decodeDouble(encoded), equalTo(v));
        assertThat(encoded.length, equalTo(0));
    }

    public void testBoolean() {
        BreakingBytesRefBuilder builder = ExtractorTests.nonBreakingBytesRefBuilder();
        boolean v = randomBoolean();
        encoder.encodeBoolean(v, builder);
        BytesRef encoded = builder.bytesRefView();
        assertThat(encoder.decodeBoolean(encoded), equalTo(v));
        assertThat(encoded.length, equalTo(0));
    }

    public void testAlpha() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef(randomAlphaOfLength(6)));
    }

    public void testUtf8() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef(randomRealisticUnicodeOfLength(6)));
    }

    /**
     * Round trip the highest unicode character to encode without a continuation.
     */
    public void testDel() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef("\u007F"));
    }

    /**
     * Round trip the lowest unicode character to encode using a continuation byte.
     */
    public void testPaddingCharacter() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef("\u0080"));
    }

    public void testVersion() {
        assumeTrue("unsupported", encoder == TopNEncoder.VERSION);
        roundTripBytesRef(randomVersion().toBytesRef());
    }

    public void testIp() {
        assumeTrue("unsupported", encoder == TopNEncoder.IP);
        roundTripBytesRef(new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))));
    }

    private void roundTripBytesRef(BytesRef v) {
        BreakingBytesRefBuilder builder = ExtractorTests.nonBreakingBytesRefBuilder();
        int reportedSize = encoder.encodeBytesRef(v, builder);
        BytesRef encoded = builder.bytesRefView();
        assertThat(encoded.length, equalTo(reportedSize));
        assertThat(encoder.decodeBytesRef(encoded, new BytesRef()), equalTo(v));
        assertThat(encoded.length, equalTo(0));
    }

    static Version randomVersion() {
        // TODO degenerate versions and stuff
        return switch (between(0, 3)) {
            case 0 -> new Version(Integer.toString(between(0, 100)));
            case 1 -> new Version(between(0, 100) + "." + between(0, 100));
            case 2 -> new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100));
            case 3 -> TopNOperatorTests.randomVersion();
            default -> throw new IllegalArgumentException();
        };
    }
}
