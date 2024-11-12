/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

public class RoutingPathFieldsTests extends ESTestCase {

    public void testWithBuilder() throws Exception {
        IndexSettings settings = new IndexSettings(
            IndexMetadata.builder("test")
                .settings(
                    indexSettings(IndexVersion.current(), 1, 1).put(
                        Settings.builder().put("index.mode", "time_series").put("index.routing_path", "path.*").build()
                    )
                )
                .build(),
            Settings.EMPTY
        );
        IndexRouting.ExtractFromSource routing = (IndexRouting.ExtractFromSource) settings.getIndexRouting();

        var routingPathFields = new RoutingPathFields(routing.builder());
        BytesReference current, previous;

        routingPathFields.addString("path.string_name", randomAlphaOfLengthBetween(1, 10));
        current = previous = routingPathFields.buildHash();
        assertNotNull(current);

        routingPathFields.addBoolean("path.boolean_name", randomBoolean());
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        previous = current;

        routingPathFields.addLong("path.long_name", randomLong());
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        previous = current;

        routingPathFields.addIp("path.ip_name", randomIp(randomBoolean()));
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        previous = current;

        routingPathFields.addUnsignedLong("path.unsigned_long_name", randomLongBetween(0, Long.MAX_VALUE));
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        assertArrayEquals(current.array(), routingPathFields.buildHash().array());
    }

    public void testWithoutBuilder() throws Exception {
        var routingPathFields = new RoutingPathFields(null);
        BytesReference current, previous;

        routingPathFields.addString("path.string_name", randomAlphaOfLengthBetween(1, 10));
        current = previous = routingPathFields.buildHash();
        assertNotNull(current);

        routingPathFields.addBoolean("path.boolean_name", randomBoolean());
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        previous = current;

        routingPathFields.addLong("path.long_name", randomLong());
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        previous = current;

        routingPathFields.addIp("path.ip_name", randomIp(randomBoolean()));
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        previous = current;

        routingPathFields.addUnsignedLong("path.unsigned_long_name", randomLongBetween(0, Long.MAX_VALUE));
        current = routingPathFields.buildHash();
        assertTrue(current.length() > previous.length());
        assertArrayEquals(current.array(), routingPathFields.buildHash().array());
    }
}
