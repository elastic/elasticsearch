/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.ByteStreams;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class HttpResponse implements Closeable {

    private int status;
    private InputStream inputStream;
    private byte[] body;

    public HttpResponse(int status) {
        this.status = status;
    }

    public int status() {
        return status;
    }

    public byte[] body() {
        if (body == null) {
            try {
                body = ByteStreams.toByteArray(inputStream);
                inputStream.close();
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
        return body;
    }

    public void inputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}