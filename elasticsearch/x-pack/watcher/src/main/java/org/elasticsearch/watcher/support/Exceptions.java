/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.script.ScriptException;

import java.io.IOException;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 *
 */
public class Exceptions {

    private Exceptions() {
    }

    public static IllegalArgumentException illegalArgument(String msg, Object... args) {
        return new IllegalArgumentException(format(msg, args));
    }

    public static IllegalArgumentException illegalArgument(String msg, Throwable cause, Object... args) {
        return new IllegalArgumentException(format(msg, args), cause);
    }

    public static IllegalStateException illegalState(String msg, Object... args) {
        return new IllegalStateException(format(msg, args));
    }

    public static IllegalStateException illegalState(String msg, Throwable cause, Object... args) {
        return new IllegalStateException(format(msg, args), cause);
    }

    public static IOException ioException(String msg, Object... args) {
        return new IOException(format(msg, args));
    }

    public static IOException ioException(String msg, Throwable cause, Object... args) {
        return new IOException(format(msg, args), cause);
    }


    //todo remove once ScriptException supports varargs
    public static ScriptException invalidScript(String msg, Object... args) {
        throw new ScriptException(format(msg, args));
    }

    //todo remove once SettingsException supports varargs
    public static SettingsException invalidSettings(String msg, Object... args) {
        throw new SettingsException(format(msg, args));
    }
}
