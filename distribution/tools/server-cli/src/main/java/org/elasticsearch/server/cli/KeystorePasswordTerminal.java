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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class KeystorePasswordTerminal extends Terminal {

    private final Terminal delegate;

    protected KeystorePasswordTerminal(Terminal delegate, SecureString password) {
        super(newPasswordReader(password), delegate.getWriter(), delegate.getErrorWriter());
        this.delegate = delegate;
    }

    private static Reader newPasswordReader(final SecureString password) {
        // copy to a buffer with the expected newline as if a user input it and hit enter
        char[] newline = System.lineSeparator().toCharArray();
        final char[] inputChars = new char[password.length() + newline.length];
        System.arraycopy(password.getChars(), 0, inputChars, 0, password.length());
        System.arraycopy(newline, 0, inputChars, password.length(), newline.length);
        password.close();

        return new Reader() {
            int pos = 0;

            @Override
            public int read(char[] cbuf, int off, int len) {
                int charsLeft = inputChars.length - pos;
                if (charsLeft == 0) {
                    close();
                    return -1;
                }
                int toCopy = Math.min(charsLeft, len);
                System.arraycopy(inputChars, pos, cbuf, off, toCopy);
                pos += toCopy;
                return toCopy;
            }

            @Override
            public void close() {
                Arrays.fill(inputChars, '\0');
            }
        };
    }

    @Override
    public OutputStream getOutputStream() {
        return delegate.getOutputStream();
    }
}
