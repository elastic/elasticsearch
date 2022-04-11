/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

//import org.elasticsearch.logging.impl.ESLogMessage;
//import org.elasticsearch.logging.impl.ParameterizedMessageImpl;
import org.elasticsearch.logging.spi.MessageFactory;

// TODO PG: I wonder if we need this. I would prefer if logger users would use String as a message, possibly some parameters suppliers
public interface Message {
    MessageFactory provider = MessageFactory.provider();

    static Message createParameterizedMessage(String format, Object[] params, Throwable throwable) {
        return provider.createParametrizedMessage(format, params, throwable);// new ParameterizedMessageImpl(format, params, throwable);
    }

    static Message createParameterizedMessage(String format, Object... params) {
        return provider.createParametrizedMessage(format, params, null);
    }

    static ESMapMessage createMapMessage(String format, Object... params) {
        return provider.createMapMessage(format, params);
    }

    String getFormattedMessage();

    String getFormat();

    Object[] getParameters();

    Throwable getThrowable();

    // /** Handles messages that consist of a format string containing '{}' to represent each replaceable token, and the parameters. */
    // // TODO: need to specify the constants,e.g. ERROR_MSG_SEPARATOR
    // static Message parameterizedMessageOf(String format, Object... params) {
    // return parameterizedMessageOf(format, params, null);
    // }
    //
    // /** Handles messages that consist of a format string containing '{}' to represent each replaceable token, and the parameters. */
    // static Message parameterizedMessageOf(String format, Object[] params, Throwable throwable) {
    // return new ParameterizedMessageImpl(format, params, throwable);
    // }
    //
    // /** Handles messages that consist of a format string conforming to java.text.MessageFormat. */
    // static Message messageFormatOf(String messagePattern, Object... parameters) {
    // return new MessageFormatMessageImpl(messagePattern, parameters);
    // }
}
