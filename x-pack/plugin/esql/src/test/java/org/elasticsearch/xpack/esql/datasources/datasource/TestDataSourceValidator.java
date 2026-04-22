/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;

/** Test-only {@link DataSourceValidator}: passes settings through with {@code secret_}-prefixed keys marked secret;
 *  throws {@link ValidationException} when constructed with {@code throwOnValidate=true}. */
public class TestDataSourceValidator implements DataSourceValidator {

    private final String type;
    private final boolean throwOnValidate;

    public TestDataSourceValidator(String type) {
        this(type, false);
    }

    public TestDataSourceValidator(String type, boolean throwOnValidate) {
        this.type = type;
        this.throwOnValidate = throwOnValidate;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        if (throwOnValidate) {
            ValidationException v = new ValidationException();
            v.addValidationError("test-induced data source validation failure");
            throw v;
        }
        Map<String, DataSourceSetting> out = new HashMap<>();
        for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
            boolean secret = e.getKey().startsWith("secret_");
            out.put(e.getKey(), new DataSourceSetting(e.getValue(), secret));
        }
        return out;
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        if (throwOnValidate) {
            ValidationException v = new ValidationException();
            v.addValidationError("test-induced dataset validation failure");
            throw v;
        }
        return new HashMap<>(datasetSettings);
    }
}
