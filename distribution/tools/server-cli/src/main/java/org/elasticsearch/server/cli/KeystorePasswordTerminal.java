/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.SecureString;

import java.io.OutputStream;
import java.io.Reader;

class KeystorePasswordTerminal extends Terminal {

    private final Terminal delegate;

    protected KeystorePasswordTerminal(Terminal delegate, SecureString password) {
        super(newPasswordReader(password), delegate.getWriter(), delegate.getErrorWriter(), delegate.getLineSeparator());
        this.delegate = delegate;
    }

    private static Reader newPasswordReader(final SecureString password) {
        return new Reader() {
            int ndx = 0;

            @Override
            public int read(char[] cbuf, int off, int len) {
                int charsLeft = password.length() - ndx;
                if (charsLeft == 0) {
                    return -1;
                }
                int toCopy = Math.min(charsLeft, len);
                System.arraycopy(password.getChars(), ndx, cbuf, off, toCopy);
                ndx += toCopy;
                return toCopy;
            }

            @Override
            public void close() {
                password.close();
            }
        };
    }

    @Override
    public OutputStream getOutputStream() {
        return delegate.getOutputStream();
    }
}
