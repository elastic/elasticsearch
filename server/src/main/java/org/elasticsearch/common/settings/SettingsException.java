/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;

/**
 * A generic failure to handle settings.
 *
 *
 */
public class SettingsException extends ElasticsearchException {

    public SettingsException(String message) {
        super(message);
    }

    public SettingsException(String message, Throwable cause) {
        super(message, cause);
    }

    public SettingsException(StreamInput in) throws IOException {
        super(in);
    }

    public SettingsException(String msg, Object... args) {
        super(msg, args);
    }
}
