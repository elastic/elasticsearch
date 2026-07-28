/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Fielddata round-trips for high-cardinality ip fields in strictly columnar mode. Ip doc values hold the 16-byte {@link InetAddressPoint}
 * encoding rather than utf8 and sort by that encoding (which equals numeric ip ordering), so the three value-shaping hooks are overridden;
 * the shared cases (sorting, duplicates, nulls, single value, all-null/empty) and the fielddata-builder wiring come from the base.
 */
public class IpColumnarArrayOrderFieldDataTests extends AbstractColumnarArrayOrderFieldDataTestCase {

    @Override
    protected String fieldTypeName() {
        return "ip";
    }

    @Override
    protected List<String> randomDistinctValues(int n) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        while (values.size() < n) {
            values.add(NetworkAddress.format(randomIp(randomBoolean())));
        }
        return new ArrayList<>(values);
    }

    @Override
    protected String decode(BytesRef value) {
        byte[] bytes = new byte[value.length];
        System.arraycopy(value.bytes, value.offset, bytes, 0, value.length);
        return NetworkAddress.format(InetAddressPoint.decode(bytes));
    }

    @Override
    protected List<String> expectedFielddataOrder(List<String> values) {
        List<String> expected = new ArrayList<>(values);
        // Fielddata sorts within a document by the binary InetAddressPoint encoding, which equals numeric ip ordering.
        expected.sort(Comparator.comparing(ip -> new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)))));
        return expected;
    }

    /**
     * A single-valued (multi_value=false) ip field in columnar mode still uses high-cardinality binary doc values, but stores them as a
     * plain sorted-unique blob rather than the in-order ArrayOrderInlineNull format, so its fielddata takes the array-order=false branch
     * of {@link IpFieldMapper.IpFieldType#fielddataBuilder}. That branch is not reachable from the base cases, which all use multi_value.
     */
    public void testSingleValuedBinaryDocValuesFielddata() throws IOException {
        String ip = randomDistinctValues(1).get(0);
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(
                b -> b.startObject("field")
                    .field("type", "ip")
                    .startObject("doc_values")
                    .field("multi_value", false)
                    .endObject()
                    .endObject()
            )
        );
        // Confirm we are exercising the plain binary doc-values path, not the in-order format the base cases cover.
        assertFalse(mapperService.documentMapper().mappers().getMapper("field").storesArrayValuesInOrder());
        assertEquals(List.of(ip), readFielddataValues(mapperService, b -> b.field("field", ip)));
    }
}
