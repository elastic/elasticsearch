/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield.datageneration;

import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.xpack.versionfield.VersionStringTestUtils;

import java.util.HashMap;

public class VersionStringDataSourceHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.VersionStringGenerator handle(DataSourceRequest.VersionStringGenerator request) {
        return new DataSourceResponse.VersionStringGenerator(VersionStringTestUtils::randomVersionString);
    }

    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        if (request.fieldType().equals("version") == false) {
            return null;
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(HashMap::new);
    }

    @Override
    public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
        if (request.fieldType().equals("version") == false) {
            return null;
        }

        return new DataSourceResponse.FieldDataGenerator(new VersionStringFieldDataGenerator(request.dataSource()));
    }

}
