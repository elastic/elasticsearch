/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A specific type of {@link SettingsException} indicating failure to load a class
 * based on a settings value.
 *
 *
 */
public class NoClassSettingsException extends SettingsException {

    public NoClassSettingsException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoClassSettingsException(StreamInput in) throws IOException {
        super(in);
    }
}
