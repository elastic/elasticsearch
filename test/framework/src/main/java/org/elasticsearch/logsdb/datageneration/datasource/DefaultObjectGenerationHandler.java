/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class DefaultObjectGenerationHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
        return new DataSourceResponse.ChildFieldGenerator() {
            @Override
            public int generateChildFieldCount() {
                return ESTestCase.randomIntBetween(0, request.specification().maxFieldCountPerLevel());
            }

            @Override
            public boolean generateNestedSubObject() {
                // Using a static 10% change, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.1;
            }

            @Override
            public boolean generateRegularSubObject() {
                // Using a static 10% change, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.1;
            }

            @Override
            public String generateFieldName() {
                return randomAlphaOfLengthBetween(1, 10);
            }
        };
    }

    @Override
    public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
        return new DataSourceResponse.FieldTypeGenerator(() -> randomFrom(FieldType.values()));
    }

    @Override
    public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
        return new DataSourceResponse.ObjectArrayGenerator(() -> {
            if (ESTestCase.randomBoolean()) {
                return Optional.of(randomIntBetween(0, 5));
            }

            return Optional.empty();
        });
    }
}
