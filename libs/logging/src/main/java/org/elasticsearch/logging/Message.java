/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

// TODO: This sucks ( to extends log4j type, but is temporary, until we replace new XXXMessage with factories )
// TODO:
// TODO PG: I wonder if we need this. I would prefer if logger users would use String as a message, possibly some parameters suppliers
public interface Message { // } extends org.apache.logging.log4j.message.Message {

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
