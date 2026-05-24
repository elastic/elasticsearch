/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.datageneration.FieldType;

public class DefaultFieldDataGeneratorHandler implements DataSourceHandler {
    public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
        var fieldType = FieldType.tryParse(request.fieldType());
        if (fieldType == null) {
            // This is a custom field type
            return null;
        }

        return new DataSourceResponse.FieldDataGenerator(fieldType.generator(request.fieldName(), request.dataSource()));
    }
}
