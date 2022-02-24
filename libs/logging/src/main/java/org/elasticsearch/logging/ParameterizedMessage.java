/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.internal.ParameterizedMessageImpl;

/** Handles messages that consist of a format string containing '{}' to represent each replaceable token, and the parameters. */
// TODO: we don't really need the public type, just a factory on Message would be sufficient.
// Done this way for now to avoid too much refactoring
    // TODO: PG I would be tempted to have a refactroing in master first to get rid of these usages somehow..
    // most of the usages probably could use a signature (String message, Supplier<?>... paramSuppliers)
public final class ParameterizedMessage implements Message {

    private final ParameterizedMessageImpl impl;

    public ParameterizedMessage(String format, Object... params) {
        this(format, params, null);
    }

    public ParameterizedMessage(String format, Object[] params, Throwable throwable) {
        impl = new ParameterizedMessageImpl(format, params, throwable);
    }

    @Override
    public String getFormattedMessage() {
        return impl.getFormattedMessage();
    }

    @Override
    public String getFormat() {
        return impl.getFormat();
    }

    @Override
    public Object[] getParameters() {
        return impl.getParameters();
    }

    @Override
    public Throwable getThrowable() {
        return impl.getThrowable();
    }
}
