/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate;

import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.FileSettingsService;

import java.nio.file.Path;

public interface ReservedState {

    static Path settingsFile(Environment environment) {
        Path configDir = environment.configDir().toAbsolutePath();
        return configDir.resolve(FileSettingsService.OPERATOR_DIRECTORY).resolve(FileSettingsService.SETTINGS_FILE_NAME);
    }
}
