/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.io.IOException;
import java.io.OutputStream;

public class PositionTrackingOutputStreamStreamOutput extends OutputStreamStreamOutput {

    private long position;

    public PositionTrackingOutputStreamStreamOutput(OutputStream out) {
        super(out);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        super.writeByte(b);
        position += 1;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        super.writeBytes(b, offset, length);
        position += length;
    }

    @Override
    public long position() throws IOException {
        return position;
    }
}
