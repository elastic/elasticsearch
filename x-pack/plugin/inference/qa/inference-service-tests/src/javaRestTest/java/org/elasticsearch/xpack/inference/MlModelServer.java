/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple model server to serve ML models.
 * The URL path corresponds to a file name in this class's resources.
 * If the file is found, its content is returned, otherwise 404.
 * Respects a range header to serve partial content.
 */
public class MlModelServer implements TestRule {

    private static final String HOST = "localhost";
    private static final Logger logger = LogManager.getLogger(MlModelServer.class);

    private int port;

    public String getUrl() {
        return new URIBuilder().setScheme("http").setHost(HOST).setPort(port).toString();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String rangeHeader = exchange.getRequestHeaders().getFirst(HttpHeaders.RANGE);
        HttpHeaderParser.Range range = rangeHeader != null ? HttpHeaderParser.parseRangeHeader(rangeHeader) : null;
        logger.info("request: {} range={}", exchange.getRequestURI().getPath(), range);

        try (InputStream is = getInputStream(exchange)) {
            int httpStatus;
            long numBytes;
            if (is == null) {
                httpStatus = HttpStatus.SC_NOT_FOUND;
                numBytes = 0;
            } else if (range == null) {
                httpStatus = HttpStatus.SC_OK;
                numBytes = is.available();
            } else {
                httpStatus = HttpStatus.SC_PARTIAL_CONTENT;
                is.skipNBytes(range.start());
                numBytes = range.end() - range.start() + 1;
            }
            logger.info("response: {} {}", exchange.getRequestURI().getPath(), httpStatus);
            exchange.sendResponseHeaders(httpStatus, numBytes);
            try (OutputStream os = exchange.getResponseBody()) {
                while (numBytes > 0) {
                    byte[] bytes = is.readNBytes((int) Math.min(1 << 20, numBytes));
                    os.write(bytes);
                    numBytes -= bytes.length;
                }
            }
        }
    }

    private InputStream getInputStream(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath().substring(1);  // Strip leading slash
        String modelId = path.substring(0, path.indexOf('.'));
        String extension = path.substring(path.indexOf('.') + 1);

        // If a model specifically optimized for some platform is requested,
        // serve the default non-optimized model instead, which is compatible.
        String defaultModelId = modelId;
        for (String platform : XPackSettings.ML_NATIVE_CODE_PLATFORMS) {
            defaultModelId = defaultModelId.replace("_" + platform, "");
        }

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(defaultModelId + "." + extension);
        if (is != null && modelId.equals(defaultModelId) == false && extension.equals("metadata.json")) {
            // When an optimized version is requested, fix the default metadata,
            // so that it contains the correct model ID.
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, is.readAllBytes())) {
                is.close();
                ModelPackageConfig packageConfig = ModelPackageConfig.fromXContentLenient(parser);
                packageConfig = new ModelPackageConfig.Builder(packageConfig).setPackedModelId(modelId).build();
                is = new ByteArrayInputStream(packageConfig.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        return is;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                logger.info("Starting ML model server");
                HttpServer server = HttpServer.create();
                while (true) {
                    port = new Random().nextInt(10000, 65536);
                    try {
                        server.bind(new InetSocketAddress(HOST, port), 1);
                    } catch (Exception e) {
                        continue;
                    }
                    break;
                }
                logger.info("Bound ML model server to port {}", port);

                ExecutorService executor = Executors.newCachedThreadPool();
                server.setExecutor(executor);
                server.createContext("/", MlModelServer.this::handle);
                server.start();

                try {
                    statement.evaluate();
                } finally {
                    logger.info("Stopping ML model server on port {}", port);
                    server.stop(1);
                    executor.shutdown();
                }
            }
        };
    }
}
