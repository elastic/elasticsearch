/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.ArrayList;
import java.util.List;

public interface ColumnProcessor extends NamedWriteable  {
    /**
     * All of the named writeables needed to deserialize the instances
     * of {@linkplain ColumnProcessor}.
     */
    static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(ColumnProcessor.class, CastProcessor.NAME, CastProcessor::new));
        entries.add(new NamedWriteableRegistry.Entry(ColumnProcessor.class, ComposeProcessor.NAME, ComposeProcessor::new));
        entries.add(new NamedWriteableRegistry.Entry(ColumnProcessor.class, DateTimeProcessor.NAME, DateTimeProcessor::new));
        entries.add(new NamedWriteableRegistry.Entry(ColumnProcessor.class,
                MathFunctionProcessor.NAME, MathFunctionProcessor::new));
        entries.add(new NamedWriteableRegistry.Entry(ColumnProcessor.class,
                MatrixFieldProcessor.NAME, MatrixFieldProcessor::new));
        return entries;
    }

    Object apply(Object r);
}
