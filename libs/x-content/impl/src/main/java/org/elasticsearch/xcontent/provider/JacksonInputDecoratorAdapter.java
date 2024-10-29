/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.InputDecorator;

import org.elasticsearch.xcontent.XContentInputDecorator;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

public class JacksonInputDecoratorAdapter extends InputDecorator {
    private final XContentInputDecorator impl;

    public JacksonInputDecoratorAdapter(XContentInputDecorator impl) {
        this.impl = impl;
    }

    @Override
    public InputStream decorate(IOContext ioContext, InputStream inputStream) throws IOException {
        return impl.decorate(inputStream);
    }

    @Override
    public InputStream decorate(IOContext ioContext, byte[] bytes, int offset, int length) throws IOException {
        return impl.decorate(bytes, offset, length);
    }

    @Override
    public DataInput decorate(IOContext ctxt, DataInput input) throws IOException {
        // Usage of this API is not expected
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader decorate(IOContext ioContext, Reader reader) throws IOException {
        // Usage of this API is not expected
        throw new UnsupportedOperationException();
    }
}
