/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoShape;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class for common literal-related functions
 */
public final class Literals {

    private Literals() {

    }

    /**
     * All custom types that are not serializable by default can be be serialized as a part of Cursor (i.e as constant in ConstantProcessor)
     * should implement NamedWriteables interface and register their de-serialization methods here.
     */
    public static Collection<? extends NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        entries.add(new NamedWriteableRegistry.Entry(IntervalDayTime.class, IntervalDayTime.NAME, IntervalDayTime::new));
        entries.add(new NamedWriteableRegistry.Entry(IntervalYearMonth.class, IntervalYearMonth.NAME, IntervalYearMonth::new));
        entries.add(new NamedWriteableRegistry.Entry(GeoShape.class, GeoShape.NAME, GeoShape::new));

        return entries;
    }
}
