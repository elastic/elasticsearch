/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IpFieldTypeTests extends FieldTypeTestCase {

    public void testValueFormat() throws Exception {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForDisplay(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForDisplay(asBytes));
    }

    public void testTermQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        String ip = "2001:db8::2:1";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, null));

        ip = "192.168.1.7";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, null));

        ip = "2001:db8::2:1";
        String prefix = ip + "/64";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 64), ft.termQuery(prefix, null));

        ip = "192.168.1.7";
        prefix = ip + "/16";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 16), ft.termQuery(prefix, null));

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType("field", false, false, true,
            null, null, Collections.emptyMap(), false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.termQuery("::1", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        assertEquals(InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
                ft.termsQuery(Arrays.asList(InetAddresses.forString("::2"), InetAddresses.forString("::5")), null));
        assertEquals(InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
                ft.termsQuery(Arrays.asList("::2", "::5"), null));

        // if the list includes a prefix query we fallback to a bool query
        assertEquals(new ConstantScoreQuery(new BooleanQuery.Builder()
                .add(ft.termQuery("::42", null), Occur.SHOULD)
                .add(ft.termQuery("::2/16", null), Occur.SHOULD).build()),
                ft.termsQuery(Arrays.asList("::42", "::2/16"), null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddressPoint.MAX_VALUE),
                ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddresses.forString("192.168.2.0")),
                ft.rangeQuery(null, "192.168.2.0", randomBoolean(), true, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddresses.forString("192.168.1.255")),
                ft.rangeQuery(null, "192.168.2.0", randomBoolean(), false, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::"),
                        InetAddressPoint.MAX_VALUE),
                ft.rangeQuery("2001:db8::", null, true, randomBoolean(), null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::1"),
                        InetAddressPoint.MAX_VALUE),
                ft.rangeQuery("2001:db8::", null, false, randomBoolean(), null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::"),
                        InetAddresses.forString("2001:db8::ffff")),
                ft.rangeQuery("2001:db8::", "2001:db8::ffff", true, true, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::1"),
                        InetAddresses.forString("2001:db8::fffe")),
                ft.rangeQuery("2001:db8::", "2001:db8::ffff", false, false, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::2"),
                        InetAddresses.forString("2001:db8::")),
                // same lo/hi values but inclusive=false so this won't match anything
                ft.rangeQuery("2001:db8::1", "2001:db8::1", false, false, null, null, null, null));

        // Upper bound is the min IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(),
                ft.rangeQuery("::", "::", true, false, null, null, null, null));

        // Lower bound is the max IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(),
                ft.rangeQuery("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                        false, true, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddresses.forString("::fffe:ffff:ffff")),
                // same lo/hi values but inclusive=false so this won't match anything
                ft.rangeQuery("::", "0.0.0.0", true, false, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::1:0:0:0"),
                        InetAddressPoint.MAX_VALUE),
                // same lo/hi values but inclusive=false so this won't match anything
                ft.rangeQuery("255.255.255.255", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false, true, null, null, null, null));

        assertEquals(
                // lower bound is ipv4, upper bound is ipv6
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("192.168.1.7"),
                        InetAddresses.forString("2001:db8::")),
                ft.rangeQuery("::ffff:c0a8:107", "2001:db8::", true, true, null, null, null, null));

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType("field", false, false, true,
            null, null, Collections.emptyMap(), false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.rangeQuery("::1", "2001::", true, true, null, null, null, null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper
            = new IpFieldMapper.Builder("field", ScriptCompiler.NONE, true, Version.CURRENT).build(new ContentPath()).fieldType();
        assertEquals(List.of("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8::2:1"));
        assertEquals(List.of("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8:0:0:0:0:2:1"));
        assertEquals(List.of("::1"), fetchSourceValue(mapper, "0:0:0:0:0:0:0:1"));

        MappedFieldType nullValueMapper = new IpFieldMapper.Builder("field", ScriptCompiler.NONE, true, Version.CURRENT)
            .nullValue("2001:db8:0:0:0:0:2:7")
            .build(new ContentPath())
            .fieldType();
        assertEquals(List.of("2001:db8::2:7"), fetchSourceValue(nullValueMapper, null));
    }
}
