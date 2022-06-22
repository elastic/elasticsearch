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
import org.elasticsearch.bootstrap.StartupException;
import org.elasticsearch.common.inject.CreationException;

/**
 * Outputs a very short version of exceptions for the console, pointing to full log for details.
 */
@Plugin(name = "consoleException", category = PatternConverter.CATEGORY)
@ConverterKeys({ "consoleException" })
public class ConsoleThrowablePatternConverter extends ThrowablePatternConverter {
    private ConsoleThrowablePatternConverter(String[] options, Configuration config) {
        super("ConsoleThrowablePatternConverter", "throwable", options, config);
    }

    /**
     * Gets an instance of the class.
     *
     * @param config  The current Configuration.
     * @return instance of class.
     */
    public static ConsoleThrowablePatternConverter newInstance(final Configuration config, final String[] options) {
        return new ConsoleThrowablePatternConverter(options, config);
    }

    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        Throwable error = event.getThrown();
        if (error == null) {
            super.format(event, toAppendTo);
            return;
        }
        if (error instanceof StartupException e) {
            error = e.getCause();
        }
        toAppendTo.append("\n\nElasticsearch failed to startup normally.\n\n");
        if (error instanceof CreationException) {
            toAppendTo.append("There were problems initializing Guice. See log for more details.");
        } else {
            toAppendTo.append(error.getMessage());
            toAppendTo.append("\n\nSee logs for more details.\n");
        }
    }
}
