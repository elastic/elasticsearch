/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IPAddressTests extends ESTestCase {

    public void testToString() {
        String v4 = "192.168.7.255";
        assertEquals(v4, new IPAddress(v4).toString());
        String v6 = "b181:3a88:339c:97f5:2b40:5175:bf3d:f77e";
        assertEquals(v6, new IPAddress(v6).toString());
    }

    public void testV4() {
        IPAddress addr4 = new IPAddress("169.254.0.0");
        assertTrue(addr4.isV4());
        assertFalse(addr4.isV6());
    }

    public void testV6() {
        IPAddress addr4 = new IPAddress("b181:3a88:339c:97f5:2b40:5175:bf3d:f77e");
        assertFalse(addr4.isV4());
        assertTrue(addr4.isV6());
    }

    public void testWriteable() throws IOException {
        var registry = new NamedWriteableRegistry(
            List.of(new Entry(GenericNamedWriteable.class, IPAddress.NAMED_WRITEABLE_NAME, IPAddress::new))
        );
        var original = new IPAddress("192.168.1.1");
        var generic = copyNamedWriteable(original, registry, GenericNamedWriteable.class);
        var copied = asInstanceOf(IPAddress.class, generic);
        assertThat(copied.toString(), equalTo(original.toString()));
    }
}
