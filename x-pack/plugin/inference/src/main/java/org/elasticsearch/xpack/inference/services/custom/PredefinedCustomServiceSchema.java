/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import java.util.List;
import java.util.Map;

public interface PredefinedCustomServiceSchema {
    List<String> getNonParameterServiceSettings();

    List<String> getServiceSettingsParameters();

    List<String> getServiceSettingsSecretParameters();

    List<String> getTaskSettingsParameters();

    String generateServiceSettings(Map<String, Object> parameters);
}
