/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.remote;

import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultTaskSettings;

import java.util.Map;
import java.util.function.Function;

public class RemoteInferenceIntegration {
    private final ServiceSettingsIntegration serviceSettingsIntegration;
    private final TaskSettingsIntegration taskSettingsIntegration;

    public RemoteInferenceIntegration(
        ServiceSettingsIntegration serviceSettingsIntegration,
        TaskSettingsIntegration taskSettingsIntegration
    ) {
        this.serviceSettingsIntegration = serviceSettingsIntegration;
        this.taskSettingsIntegration = taskSettingsIntegration;
    }

    public DefaultSecretSettings parseSecretSettings(Map<String, Object> config) {
        return DefaultSecretSettings.fromMap(config);
    }

    public DefaultServiceSettings parseServiceSettings(Map<String, Object> config) {
        return serviceSettingsIntegration.apply(config);
    }

    public DefaultTaskSettings parseTaskSettings(Map<String, Object> config) {
        return taskSettingsIntegration.apply(config);
    }

    public interface ServiceSettingsIntegration extends Function<Map<String, Object>, DefaultServiceSettings> {}

    public interface TaskSettingsIntegration extends Function<Map<String, Object>, DefaultTaskSettings> {}
}
