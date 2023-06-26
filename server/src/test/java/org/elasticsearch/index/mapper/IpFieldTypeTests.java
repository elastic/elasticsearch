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
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.mapper.IpFieldMapper.IpFieldType.convertToDocValuesQuery;

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
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, MOCK_CONTEXT));

        ip = "192.168.1.7";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, MOCK_CONTEXT));

        ip = "2001:db8::2:1";
        String prefix = ip + "/64";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 64), ft.termQuery(prefix, MOCK_CONTEXT));

        ip = "192.168.1.7";
        prefix = ip + "/16";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 16), ft.termQuery(prefix, MOCK_CONTEXT));

        ft = new IpFieldMapper.IpFieldType("field", false);

        ip = "2001:db8::2:1";
        assertEquals(
            convertToDocValuesQuery(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip))),
            ft.termQuery(ip, MOCK_CONTEXT)
        );

        ip = "192.168.1.7";
        assertEquals(
            convertToDocValuesQuery(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip))),
            ft.termQuery(ip, MOCK_CONTEXT)
        );

        ip = "2001:db8::2:1";
        prefix = ip + "/64";
        assertEquals(
            convertToDocValuesQuery(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 64)),
            ft.termQuery(prefix, MOCK_CONTEXT)
        );

        ip = "192.168.1.7";
        prefix = ip + "/16";
        assertEquals(
            convertToDocValuesQuery(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 16)),
            ft.termQuery(prefix, MOCK_CONTEXT)
        );

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType(
            "field",
            false,
            false,
            false,
            null,
            null,
            Collections.emptyMap(),
            false
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("::1", MOCK_CONTEXT));
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        assertEquals(
            InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
            ft.termsQuery(Arrays.asList(InetAddresses.forString("::2"), InetAddresses.forString("::5")), MOCK_CONTEXT)
        );
        assertEquals(
            InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
            ft.termsQuery(Arrays.asList("::2", "::5"), MOCK_CONTEXT)
        );

        // if the list includes a prefix query we fallback to a bool query
        assertEquals(
            new ConstantScoreQuery(
                new BooleanQuery.Builder().add(ft.termQuery("::42", MOCK_CONTEXT), Occur.SHOULD)
                    .add(ft.termQuery("::2/16", null), Occur.SHOULD)
                    .build()
            ),
            ft.termsQuery(Arrays.asList("::42", "::2/16"), MOCK_CONTEXT)
        );
    }

    public void testRangeQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        Query query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("192.168.2.0"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery(null, "192.168.2.0", randomBoolean(), true, null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("192.168.1.255"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery(null, "192.168.2.0", randomBoolean(), false, null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery("2001:db8::", null, true, randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::1"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery("2001:db8::", null, false, randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::"), InetAddresses.forString("2001:db8::ffff"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery("2001:db8::", "2001:db8::ffff", true, true, null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::1"), InetAddresses.forString("2001:db8::fffe"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery("2001:db8::", "2001:db8::ffff", false, false, null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::2"), InetAddresses.forString("2001:db8::"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("2001:db8::1", "2001:db8::1", false, false, null, null, null, MOCK_CONTEXT)
        );

        // Upper bound is the min IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("::", "::", true, false, null, null, null, MOCK_CONTEXT));

        // Lower bound is the max IP and is not inclusive
        assertEquals(
            new MatchNoDocsQuery(),
            ft.rangeQuery(
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                false,
                true,
                null,
                null,
                null,
                MOCK_CONTEXT
            )
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("::fffe:ffff:ffff"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("::", "0.0.0.0", true, false, null, null, null, MOCK_CONTEXT)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::1:0:0:0"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("255.255.255.255", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false, true, null, null, null, MOCK_CONTEXT)
        );

        // lower bound is ipv4, upper bound is ipv6
        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("192.168.1.7"), InetAddresses.forString("2001:db8::"));
        assertEquals(
            new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query)),
            ft.rangeQuery("::ffff:c0a8:107", "2001:db8::", true, true, null, null, null, MOCK_CONTEXT)
        );

        ft = new IpFieldMapper.IpFieldType("field", true, false);

        assertEquals(
            InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddressPoint.MAX_VALUE),
            ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        ft = new IpFieldMapper.IpFieldType("field", false);

        assertEquals(
            convertToDocValuesQuery(InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddressPoint.MAX_VALUE)),
            ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("192.168.2.0"))
            ),
            ft.rangeQuery(null, "192.168.2.0", randomBoolean(), true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("192.168.1.255"))
            ),
            ft.rangeQuery(null, "192.168.2.0", randomBoolean(), false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::"), InetAddressPoint.MAX_VALUE)
            ),
            ft.rangeQuery("2001:db8::", null, true, randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::1"), InetAddressPoint.MAX_VALUE)
            ),
            ft.rangeQuery("2001:db8::", null, false, randomBoolean(), null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::"), InetAddresses.forString("2001:db8::ffff"))
            ),
            ft.rangeQuery("2001:db8::", "2001:db8::ffff", true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::1"), InetAddresses.forString("2001:db8::fffe"))
            ),
            ft.rangeQuery("2001:db8::", "2001:db8::ffff", false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::2"), InetAddresses.forString("2001:db8::"))
            ),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("2001:db8::1", "2001:db8::1", false, false, null, null, null, MOCK_CONTEXT)
        );

        // Upper bound is the min IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("::", "::", true, false, null, null, null, MOCK_CONTEXT));

        // Lower bound is the max IP and is not inclusive
        assertEquals(
            new MatchNoDocsQuery(),
            ft.rangeQuery(
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                false,
                true,
                null,
                null,
                null,
                MOCK_CONTEXT
            )
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("::fffe:ffff:ffff"))
            ),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("::", "0.0.0.0", true, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::1:0:0:0"), InetAddressPoint.MAX_VALUE)
            ),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("255.255.255.255", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            // lower bound is ipv4, upper bound is ipv6
            convertToDocValuesQuery(
                InetAddressPoint.newRangeQuery("field", InetAddresses.forString("192.168.1.7"), InetAddresses.forString("2001:db8::"))
            ),
            ft.rangeQuery("::ffff:c0a8:107", "2001:db8::", true, true, null, null, null, MOCK_CONTEXT)
        );

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType(
            "field",
            false,
            false,
            false,
            null,
            null,
            Collections.emptyMap(),
            false
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery("::1", "2001::", true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new IpFieldMapper.Builder("field", ScriptCompiler.NONE, true, IndexVersion.CURRENT).build(
            MapperBuilderContext.root(false)
        ).fieldType();
        assertEquals(List.of("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8::2:1"));
        assertEquals(List.of("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8:0:0:0:0:2:1"));
        assertEquals(List.of("::1"), fetchSourceValue(mapper, "0:0:0:0:0:0:0:1"));

        MappedFieldType nullValueMapper = new IpFieldMapper.Builder("field", ScriptCompiler.NONE, true, IndexVersion.CURRENT).nullValue(
            "2001:db8:0:0:0:0:2:7"
        ).build(MapperBuilderContext.root(false)).fieldType();
        assertEquals(List.of("2001:db8::2:7"), fetchSourceValue(nullValueMapper, null));
    }
}
