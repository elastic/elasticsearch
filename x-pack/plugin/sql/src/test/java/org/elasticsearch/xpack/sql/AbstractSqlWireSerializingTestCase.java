/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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
    protected T copyInstance(T instance, Version version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            ZoneId zoneId = instanceZoneId(instance);
            SqlStreamOutput out = new SqlStreamOutput(version, zoneId);
            instance.writeTo(out);
            out.close();
            try (SqlStreamInput in = new SqlStreamInput(out.streamAsString(), getNamedWriteableRegistry(), version)) {
                return instanceReader().read(in);
            }
        }
    }

    protected ZoneId instanceZoneId(T instance) {
        return randomSafeZone();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
    }


    /**
     * We need to exclude SystemV/* time zones because they cannot be converted
     * back to DateTimeZone which we currently still need to do internally,
     * e.g. in bwc serialization and in the extract() method
     */
    protected static ZoneId randomSafeZone() {
        return randomValueOtherThanMany(zi -> zi.getId().startsWith("SystemV"), () -> randomZone());
    }
}
