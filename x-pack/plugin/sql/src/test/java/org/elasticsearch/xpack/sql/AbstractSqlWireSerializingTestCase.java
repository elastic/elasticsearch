/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.common.io.SqlStreamOutput;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.io.IOException;
import java.time.ZoneId;

public abstract class AbstractSqlWireSerializingTestCase<T extends Writeable> extends AbstractWireTestCase<T> {

    @Override
    protected T copyInstance(T instance, TransportVersion version) throws IOException {
        ZoneId zoneId = instanceZoneId(instance);
        SqlStreamOutput out = SqlStreamOutput.create(version, zoneId);
        instance.writeTo(out);
        out.close();
        try (SqlStreamInput in = SqlStreamInput.fromString(out.streamAsString(), getNamedWriteableRegistry(), version)) {
            return instanceReader().read(in);
        }
    }

    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    protected abstract Writeable.Reader<T> instanceReader();

    protected ZoneId instanceZoneId(T instance) {
        return randomZone();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
    }
}
