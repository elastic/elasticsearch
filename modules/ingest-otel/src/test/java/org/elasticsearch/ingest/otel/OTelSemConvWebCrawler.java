/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class OTelSemConvWebCrawler {

    private static final String OTEL_SEMCONV_ROOT_API_URL =
        "https://api.github.com/repos/open-telemetry/semantic-conventions/contents/model?ref=main";

    private static final String GITHUB_TOKEN = System.getenv("GITHUB_TOKEN");

    /**
     * Relies on GitHub API to crawl the OpenTelemetry semantic conventions repo.
     * This method blocks until all resource attributes are collected, which may take a while.
     * @return a set of resource attribute names
     */
    static Set<String> collectOTelSemConvResourceAttributes() {
        Set<String> resourceAttributes = new HashSet<>();
        try (
            // using a relatively high thread pool size, as most tasks it would execute are IO-bound
            ExecutorService executor = Executors.newFixedThreadPool(50);
            HttpClient httpClient = HttpClient.newBuilder().executor(executor).build();
            Stream<String> yamlFileDownloadUrls = findAllYamlFiles(httpClient)
        ) {
            // collect all futures without blocking
            List<CompletableFuture<Set<String>>> futures = yamlFileDownloadUrls.map(
                url -> extractResourceAttributesFromFile(url, httpClient)
            ).toList();

            // wait for all to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();

            // collect results
            futures.forEach(future -> resourceAttributes.addAll(future.join()));
        }
        return resourceAttributes;
    }

    /**
     * Recursively find all yaml files in the OpenTelemetry semantic-conventions repo.
     * The result stream is returned immediately, and the processing is done in separate HttpClient threads.
     * @return a stream of yaml file download URLs
     */
    private static Stream<String> findAllYamlFiles(HttpClient client) {

        // using a single CompletableFuture that will be completed when all processing is done
        CompletableFuture<Void> allDone = new CompletableFuture<>();

        // track pending futures to know when we're done
        Set<Object> pendingTasks = ConcurrentHashMap.newKeySet();

        // continuously collect directory URLs as we encounter them
        BlockingQueue<String> directoryQueue = new LinkedBlockingQueue<>();
        // add the root directory to the queue
        directoryQueue.add(OTEL_SEMCONV_ROOT_API_URL);

        BlockingQueue<String> resultsQueue = new LinkedBlockingQueue<>();

        // processing is done in a separate thread so that the stream can be returned immediately
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            CompletableFuture.runAsync(() -> {
                try {
                    while (allDone.isDone() == false) {
                        // adding task to the pending tasks set to mark that we're processing a directory
                        Object currentDirectoryProcessingTask = new Object();
                        // this prevents race conditions where one thread processes a directory while another thread queries the pending
                        // tasks before the first thread has added the corresponding http processing tasks to the pending tasks set
                        pendingTasks.add(currentDirectoryProcessingTask);
                        try {
                            String currentUrl = directoryQueue.poll(100, TimeUnit.MILLISECONDS);

                            if (currentUrl == null) {
                                continue;
                            }

                            HttpRequest request = createRequest(currentUrl);
                            // fork the HTTP request to a separate thread
                            CompletableFuture<HttpResponse<InputStream>> responseFuture = client.sendAsync(
                                request,
                                HttpResponse.BodyHandlers.ofInputStream()
                            );

                            CompletableFuture<?> responseProcessingTask = responseFuture.thenApply(response -> {
                                try {
                                    if (response.statusCode() == 200) {
                                        try (
                                            InputStream inputStream = response.body();
                                            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                                .createParser(XContentParserConfiguration.EMPTY, inputStream)
                                        ) {
                                            List<?> items = parser.list();
                                            for (Object item : items) {
                                                if (item instanceof Map<?, ?> entry) {
                                                    String type = (String) entry.get("type");
                                                    String name = (String) entry.get("name");
                                                    String downloadUrl = (String) entry.get("download_url");
                                                    String url = (String) entry.get("url");

                                                    if ("file".equals(type) && (name.endsWith(".yaml") || name.endsWith(".yml"))) {
                                                        if (downloadUrl != null) {
                                                            resultsQueue.add(downloadUrl);
                                                        }
                                                    } else if ("dir".equals(type) && url != null) {
                                                        directoryQueue.add(url);
                                                    }
                                                }
                                            }
                                        }
                                    } else if (response.statusCode() == 403) {
                                        System.err.println(
                                            "GitHub API rate limit exceeded. "
                                                + "Please provide a GitHub token via GITHUB_TOKEN environment variable"
                                        );
                                    } else {
                                        System.err.println("GitHub API request failed: HTTP " + response.statusCode());
                                    }
                                } catch (IOException e) {
                                    System.err.println("Error processing response: " + e.getMessage());
                                }
                                return null;
                            });
                            pendingTasks.add(responseProcessingTask);

                            // when this future completes, remove it from pending
                            responseProcessingTask.whenComplete((result, ex) -> {
                                pendingTasks.remove(responseProcessingTask);
                                markDoneIfRequired(pendingTasks, directoryQueue, allDone);
                            });
                        } finally {
                            // it's now safe to remove the current directory processing task from pending tasks as the
                            // corresponding response processing tasks, if any, have already been added to the pending tasks set
                            pendingTasks.remove(currentDirectoryProcessingTask);
                            markDoneIfRequired(pendingTasks, directoryQueue, allDone);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    allDone.completeExceptionally(e);
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    allDone.completeExceptionally(e);
                    throw new RuntimeException(e);
                }
            }, executor);
        }

        // create a custom spliterator that pulls from the queue
        Spliterator<String> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super String> action) {
                try {
                    String url = resultsQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (url != null) {
                        action.accept(url);
                        return true;
                    }
                    return allDone.isDone() == false || resultsQueue.isEmpty() == false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        };

        // not using parallel stream here because we want to control the concurrency with our own thread pool
        return StreamSupport.stream(spliterator, false).onClose(() -> {
            // cleanup when the stream is closed
            allDone.complete(null);
        });
    }

    private static CompletableFuture<Set<String>> extractResourceAttributesFromFile(String fileDownloadUrl, HttpClient httpClient) {
        return downloadAndParseYaml(fileDownloadUrl, httpClient).thenApply(yamlData -> {
            Set<String> resourceAttributes = new HashSet<>();
            // look for 'groups' entry
            Object groupsObj = yamlData.get("groups");
            if (groupsObj instanceof List<?> groups) {
                for (Object groupObj : groups) {
                    if (groupObj instanceof Map<?, ?> groupMap) {
                        // check if 'type' is 'resource'
                        if ("resource".equals(groupMap.get("type"))) {
                            // Get 'attributes' and extract 'ref' fields
                            Object attributesObj = groupMap.get("attributes");
                            if (attributesObj instanceof List<?> attributesList && attributesList.isEmpty() == false) {
                                for (Object attributeObj : attributesList) {
                                    if (attributeObj instanceof Map<?, ?> attributeMap) {
                                        String refVal = (String) attributeMap.get("ref");
                                        if (refVal != null) {
                                            resourceAttributes.add(refVal);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return resourceAttributes;
        });
    }

    private static void markDoneIfRequired(
        Set<Object> pendingTasks,
        BlockingQueue<String> directoryQueue,
        CompletableFuture<Void> allDone
    ) {
        // There are two types of tasks: directory processing tasks and response processing tasks.
        // The only thing that can add to the directory queue is a response processing task, which can only be created
        // during the processing of a directory.
        // Therefore, if the directory queue is empty and there are no pending tasks - we are done.
        if (pendingTasks.isEmpty() && directoryQueue.isEmpty()) {
            allDone.complete(null);
        }
    }

    private static HttpRequest createRequest(String currentUrl) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(currentUrl)).header("Accept", "application/vnd.github+json");

        if (GITHUB_TOKEN != null && GITHUB_TOKEN.isEmpty() == false) {
            requestBuilder.header("Authorization", "Bearer " + GITHUB_TOKEN);
        }

        return requestBuilder.build();
    }

    private static CompletableFuture<Map<String, Object>> downloadAndParseYaml(String rawFileUrl, HttpClient client) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(rawFileUrl)).build();
        CompletableFuture<HttpResponse<InputStream>> responseFuture = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
        return responseFuture.thenApply(response -> {
            try (
                InputStream inputStream = response.body();
                XContentParser parser = XContentFactory.xContent(XContentType.YAML)
                    .createParser(XContentParserConfiguration.EMPTY, inputStream)
            ) {
                return parser.map();
            } catch (IOException e) {
                System.err.println("Error parsing YAML file: " + e.getMessage());
                return Map.of();
            } finally {
                if (response.statusCode() != 200) {
                    System.err.println("GitHub API request failed: HTTP " + response.statusCode());
                }
            }
        });
    }
}
