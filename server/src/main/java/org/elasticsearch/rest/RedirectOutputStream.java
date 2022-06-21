/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.elasticsearch.common.io.stream.BytesStream;

import java.io.IOException;
import java.io.OutputStream;

public final class RedirectOutputStream extends OutputStream {

    private BytesStream target;

    public void newTarget(BytesStream target) {
        assert this.target == null;
        this.target = target;
    }

    public void clearTarget() {
        this.target = null;
    }

    public BytesStream getTarget() {
        return target;
    }


    @Override
    public void write(int b) throws IOException {
        target.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        target.write(b, off, len);
    }
}
