/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.ValueFormatter;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;

import static java.util.Collections.emptyList;

public class ItemSetMapReduceValueSourceTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    // helper method to create fields for testing in other namespaces
    public static Field createKeywordFieldTestInstance(String name, int id) {
        return new Field(name, id, DocValueFormat.RAW, ValueFormatter.BYTES_REF);
    }

    public static Field createIpFieldTestInstance(String name, int id) {
        return new Field(name, id, DocValueFormat.IP, ValueFormatter.BYTES_REF);
    }

    public static Field createLongFieldTestInstance(String name, int id) {
        return new Field(name, id, DocValueFormat.RAW, ValueFormatter.LONG);
    }

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return namedWriteableRegistry;
    }

    public void testKeyword() throws IOException {
        Field field = createKeywordFieldTestInstance(randomAlphaOfLengthBetween(3, 20), 0);
        assertEquals("somethingü+e개", copyField(field).formatValue(new BytesRef("somethingü+e개")));
    }

    public void testDate() throws IOException {
        Field field = new Field(
            randomAlphaOfLengthBetween(3, 20),
            0,
            new DocValueFormat.DateTime(
                DateFormatter.forPattern("date_hour_minute_second"),
                ZoneOffset.ofHours(1),
                Resolution.MILLISECONDS
            ),
            ValueFormatter.LONG
        );
        assertEquals("2022-07-07T21:32:41", copyField(field).formatValue(1657225961000L));
    }

    public void testIp() throws IOException {
        Field field = createIpFieldTestInstance(randomAlphaOfLengthBetween(3, 20), 0);
        assertEquals(
            "192.168.3.44",
            copyField(field).formatValue(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.3.44"))))
        );
    }

    public void testLong() throws IOException {
        Field field = createLongFieldTestInstance(randomAlphaOfLengthBetween(3, 20), 0);
        assertEquals(42L, copyField(field).formatValue(Long.valueOf(42L)));
    }

    private Field copyField(Field field) throws IOException {
        return copyInstance(field, writableRegistry(), (out, value) -> value.writeTo(out), Field::new, TransportVersion.current());
    }
}
