/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.elasticsearch.index.codec.tsdb.pipeline.bench.NumericDataGenerators;
import org.elasticsearch.index.codec.tsdb.pipeline.bench.NumericDataGenerators.SeededDoubleDataSource;
import org.elasticsearch.index.codec.tsdb.pipeline.bench.NumericDataGenerators.SeededLongDataSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

// NOTE: Adapter that converts NumericDataGenerators seeded data sources into
// NamedSupplier lists for JMH benchmarks. Data is generated eagerly; the
// returned supplier calls base.clone() for mutation safety. Clones happen
// per trial (inside @Setup), not per benchmark invocation.
public final class NumericDataSupplierRegistry {

    private NumericDataSupplierRegistry() {}

    public static List<NamedSupplier> longSuppliers(int size, long seed) {
        final List<NamedSupplier> result = new ArrayList<>();
        for (final SeededLongDataSource ds : NumericDataGenerators.seededLongDataSources()) {
            final long[] base = ds.generator().apply(size, seed);
            result.add(new NamedSupplier(ds.name(), base::clone));
        }
        return Collections.unmodifiableList(result);
    }

    public static List<NamedSupplier> doubleSuppliers(int size, long seed) {
        final List<NamedSupplier> result = new ArrayList<>();
        for (final SeededDoubleDataSource ds : NumericDataGenerators.seededDoubleDataSources()) {
            final long[] base = NumericDataGenerators.doublesToSortableLongs(ds.generator().apply(size, seed));
            result.add(new NamedSupplier(ds.name(), base::clone));
        }
        return Collections.unmodifiableList(result);
    }

    // NOTE: Builds a name->supplier map from a list. Use toUnmodifiableMap to
    // prevent accidental mutation. Throws on duplicate keys.
    public static Map<String, Supplier<long[]>> toMap(List<NamedSupplier> suppliers) {
        return suppliers.stream().collect(Collectors.toUnmodifiableMap(NamedSupplier::name, NamedSupplier::supplier));
    }
}
