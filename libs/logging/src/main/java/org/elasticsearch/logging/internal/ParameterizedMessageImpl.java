/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.logging.Message;

public class ParameterizedMessageImpl extends ParameterizedMessage implements Message {

    public ParameterizedMessageImpl(String format, Object[] params, Throwable throwable) {
        super(format, params, throwable);
    }
}
