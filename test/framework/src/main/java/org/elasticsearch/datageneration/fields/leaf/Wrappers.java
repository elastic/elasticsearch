/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.fields.leaf;

import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;

import java.util.function.Supplier;

public class Wrappers {
    /**
     * Applies default wrappers for raw values - adds nulls and wraps values in arrays.
     * @return
     */
    public static Supplier<Object> defaults(Supplier<Object> rawValues, DataSource dataSource) {
        var nulls = dataSource.get(new DataSourceRequest.NullWrapper());
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper());

        return arrays.wrapper().compose(nulls.wrapper()).apply(rawValues::get);
    }

    /**
     * Applies default wrappers for raw values and also adds malformed values.
     * @return
     */
    public static Supplier<Object> defaultsWithMalformed(
        Supplier<Object> rawValues,
        Supplier<Object> malformedValues,
        DataSource dataSource
    ) {
        var nulls = dataSource.get(new DataSourceRequest.NullWrapper());
        var malformed = dataSource.get(new DataSourceRequest.MalformedWrapper(malformedValues));
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper());

        return arrays.wrapper().compose(nulls.wrapper()).compose(malformed.wrapper()).apply(rawValues::get);
    }
}
