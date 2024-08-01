/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class handles any decision performed during data generation that changes the output.
 * For example: generating a random number, array of random size, mapping parameter.
 * <p>
 * Goals of this abstraction are:
 * <ul>
 * <li> to be able to easily add new types of decisions/generators </li>
 * <li> to decouple different types of decisions from each other, adding new data type should be an isolated additive change </li>
 * <li> to allow overriding only small specific subset of behavior (e.g. for testing purposes) </li>
 * </ul>
 */
public class DataSource {
    private List<DataSourceHandler> handlers;

    public DataSource(Collection<DataSourceHandler> additionalHandlers) {
        this.handlers = new ArrayList<>();

        this.handlers.addAll(additionalHandlers);

        this.handlers.add(new DefaultPrimitiveTypesHandler());
        this.handlers.add(new DefaultWrappersHandler());
        this.handlers.add(new DefaultObjectGenerationHandler());
    }

    public <T extends DataSourceResponse> T get(DataSourceRequest<T> request) {
        for (var handler : handlers) {
            var response = request.accept(handler);
            if (response != null) {
                return response;
            }
        }

        throw new IllegalStateException("Request is not supported by data source");
    }
}
