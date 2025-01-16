/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.HttpStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple model server to serve ML models.
 * The URL path corresponds to file name in this class's resources.
 * If the file is found, its content is returned, otherwise 404.
 * Respects a range header to serve partial content.
 */
class MlModelServer {

    private static final Logger logger = LogManager.getLogger(MlModelServer.class);

    private final HttpServer mlModelServer;
    private final ExecutorService mlModelServerExecutor;

    MlModelServer(int port) throws IOException {
        mlModelServer = HttpServer.create(new InetSocketAddress(port), 10);
        mlModelServer.createContext("/", this::handle);
        mlModelServerExecutor = Executors.newCachedThreadPool();
        mlModelServer.setExecutor(mlModelServerExecutor);
        mlModelServer.start();
    }

    void close() {
        mlModelServer.stop(5);
        mlModelServerExecutor.close();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String fileName = exchange.getRequestURI().getPath().substring(1);
        String range = exchange.getRequestHeaders().getFirst("Range");
        Integer rangeFrom = null;
        Integer rangeTo = null;
        if (range != null) {
            assert range.startsWith("bytes=");
            assert range.contains("-");
            rangeFrom = Integer.parseInt(range.substring("bytes=".length(), range.indexOf('-')));
            rangeTo = Integer.parseInt(range.substring(range.indexOf('-') + 1)) + 1;
        }
        logger.info("Request: {} range=[{},{})", fileName, rangeFrom, rangeTo);
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        try (InputStream is = classloader.getResourceAsStream(fileName)) {
            if (is == null) {
                logger.info("Response: {} 404", fileName);
                exchange.sendResponseHeaders(HttpStatus.SC_NOT_FOUND, 0);
            } else {
                try (OutputStream os = exchange.getResponseBody()) {
                    int httpStatus;
                    int numBytes;
                    if (range == null) {
                        httpStatus = HttpStatus.SC_OK;
                        numBytes = is.available();
                    } else {
                        httpStatus = HttpStatus.SC_PARTIAL_CONTENT;
                        is.skipNBytes(rangeFrom);
                        numBytes = rangeTo - rangeFrom;
                    }
                    logger.info("Response: {} {}", fileName, httpStatus);
                    exchange.sendResponseHeaders(httpStatus, numBytes);
                    while (numBytes > 0) {
                        byte[] bytes = is.readNBytes(Math.min(1<<20, numBytes));
                        os.write(bytes);
                        numBytes -= bytes.length;
                    }
                }
            }
        }
    }
}
