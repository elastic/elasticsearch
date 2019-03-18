/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PagingListCursorTests extends AbstractWireSerializingTestCase<PagingListCursor> {
    public static PagingListCursor randomPagingListCursor() {
        int size = between(1, 20);
        int depth = between(1, 20);

        List<List<?>> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(Arrays.asList(randomArray(depth, s -> new Object[depth], () -> randomByte())));
        }

        return new PagingListCursor(values, depth, between(1, 20));
    }

    @Override
    protected PagingListCursor mutateInstance(PagingListCursor instance) throws IOException {
        return new PagingListCursor(instance.data(), 
                instance.columnCount(),
                randomValueOtherThan(instance.pageSize(), () -> between(1, 20)));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
    }

    @Override
    protected PagingListCursor createTestInstance() {
        return randomPagingListCursor();
    }

    @Override
    protected Reader<PagingListCursor> instanceReader() {
        return PagingListCursor::new;
    }

    @Override
    protected PagingListCursor copyInstance(PagingListCursor instance, Version version) throws IOException {
        /* Randomly choose between internal protocol round trip and String based
         * round trips used to toXContent. */
        if (randomBoolean()) {
            return super.copyInstance(instance, version);
        }
        return (PagingListCursor) Cursors.decodeFromString(Cursors.encodeToString(version, instance));
    }
}