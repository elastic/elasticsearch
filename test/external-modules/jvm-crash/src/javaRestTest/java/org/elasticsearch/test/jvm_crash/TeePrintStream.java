/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.jvm_crash;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Copies output to another {@code PrintStream}, as well as an {@code OutputStream}
 */
class TeePrintStream extends PrintStream {
    private final PrintStream delegate;

    TeePrintStream(PrintStream delegate, OutputStream out) {
        super(out);
        this.delegate = delegate;
    }

    @Override
    public void flush() {
        delegate.flush();
        super.flush();
    }

    @Override
    public void close() {
        delegate.close();
        super.close();
    }

    @Override
    public boolean checkError() {
        return delegate.checkError() || super.checkError();
    }

    @Override
    public void write(int b) {
        delegate.write(b);
        super.write(b);
    }

    @Override
    public void write(byte[] buf, int off, int len) {
        delegate.write(buf, off, len);
        super.write(buf, off, len);
    }

    @Override
    public void print(boolean b) {
        delegate.print(b);
        super.print(b);
    }

    @Override
    public void print(char c) {
        delegate.print(c);
        super.print(c);
    }

    @Override
    public void print(int i) {
        delegate.print(i);
        super.print(i);
    }

    @Override
    public void print(long l) {
        delegate.print(l);
        super.print(l);
    }

    @Override
    public void print(float f) {
        delegate.print(f);
        super.print(f);
    }

    @Override
    public void print(double d) {
        delegate.print(d);
        super.print(d);
    }

    @Override
    public void print(char[] s) {
        delegate.print(s);
        super.print(s);
    }

    @Override
    public void print(String s) {
        delegate.print(s);
        super.print(s);
    }

    @Override
    public void print(Object obj) {
        delegate.print(obj);
        super.print(obj);
    }

    @Override
    public void println() {
        delegate.println();
        super.println();
    }
}
