/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.pattern.ThrowablePatternConverter;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.bootstrap.StartupException;
import org.elasticsearch.common.inject.CreationException;

/**
 * Outputs a very short version of exceptions for an interactive console, pointing to full log for details.
 *
 * <p> If a non-interactive console is attached, the full exception is always printed.
 */
@Plugin(name = "consoleException", category = PatternConverter.CATEGORY)
@ConverterKeys({ "consoleException" })
public class ConsoleThrowablePatternConverter extends ThrowablePatternConverter {

    // true if exceptions should be truncated, false if they should be delegated to the super class
    private final boolean enabled;

    private ConsoleThrowablePatternConverter(String[] options, Configuration config, boolean enabled) {
        super("ConsoleThrowablePatternConverter", "throwable", options, config);
        this.enabled = enabled;
    }

    /**
     * Gets an instance of the class.
     *
     * @param config  The current Configuration.
     * @return instance of class.
     */
    public static ConsoleThrowablePatternConverter newInstance(final Configuration config, final String[] options) {
        return newInstance(config, options, BootstrapInfo.getConsole() != null);
    }

    // package private for tests
    static ConsoleThrowablePatternConverter newInstance(final Configuration config, final String[] options, boolean enabled) {
        return new ConsoleThrowablePatternConverter(options, config, enabled);
    }

    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        Throwable error = event.getThrown();
        if (enabled == false || error == null) {
            super.format(event, toAppendTo);
            return;
        }
        if (error instanceof StartupException e) {
            error = e.getCause();
            toAppendTo.append("\n\nElasticsearch failed to startup normally.\n\n");
        }

        appendShortStacktrace(error, toAppendTo);
        if (error instanceof CreationException) {
            toAppendTo.append("There were problems initializing Guice. See log for more details.");
        } else {
            toAppendTo.append("\n\nSee logs for more details.\n");
        }
    }

    // prints a very truncated stack trace, leaving the rest of the details for the log
    private static void appendShortStacktrace(Throwable error, StringBuilder toAppendTo) {
        toAppendTo.append(error.getClass().getName());
        toAppendTo.append(": ");
        toAppendTo.append(error.getMessage());

        var stacktrace = error.getStackTrace();
        int len = Math.min(stacktrace.length, 5);
        for (int i = 0; i < len; ++i) {
            toAppendTo.append("\n\tat ");
            toAppendTo.append(stacktrace[i].toString());
        }
    }
}
