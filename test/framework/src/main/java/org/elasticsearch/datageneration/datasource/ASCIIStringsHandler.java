/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;

/**
 * A {@link DataSourceHandler} that causes generated strings to be ASCII-only instead of full Unicode. Useful for debugging.
 */
public class ASCIIStringsHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
        return new DefaultObjectGenerationHandler.DefaultChildFieldGenerator(request) {
            @Override
            public String generateFieldName() {
                return ESTestCase.randomAlphaOfLengthBetween(1, 10);
            }
        };
    }

    @Override
    public DataSourceResponse.StringGenerator handle(DataSourceRequest.StringGenerator request) {
        return new DataSourceResponse.StringGenerator(() -> ESTestCase.randomAlphaOfLengthBetween(0, 50));
    }
}
