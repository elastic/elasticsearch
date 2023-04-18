/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.plugin.CursorTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListCursorTests extends AbstractSqlWireSerializingTestCase<ListCursor> {
    public static ListCursor randomPagingListCursor() {
        int size = between(1, 100);
        int depth = between(1, 20);

        List<List<?>> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(Arrays.asList(randomArray(depth, s -> new Object[depth], () -> randomByte())));
        }

        return new ListCursor(values, between(1, 20), depth);
    }

    @Override
    protected ListCursor mutateInstance(ListCursor instance) {
        return new ListCursor(instance.data(), randomValueOtherThan(instance.pageSize(), () -> between(1, 20)), instance.columnCount());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
    }

    @Override
    protected ListCursor createTestInstance() {
        return randomPagingListCursor();
    }

    @Override
    protected Writeable.Reader<ListCursor> instanceReader() {
        return ListCursor::new;
    }

    @Override
    protected ListCursor copyInstance(ListCursor instance, TransportVersion version) throws IOException {
        /* Randomly choose between internal protocol round trip and String based
         * round trips used to toXContent. */
        if (randomBoolean()) {
            return copyWriteable(instance, getNamedWriteableRegistry(), ListCursor::new, version);
        }
        return (ListCursor) CursorTests.decodeFromString(Cursors.encodeToString(instance, randomZone()));
    }
}
