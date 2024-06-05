/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;

import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.lang.String.format;

final class GoogleCloudStorageHttpStatsCollector implements HttpResponseInterceptor {
    // The specification for the current API (v1) endpoints can be found at:
    // https://cloud.google.com/storage/docs/json_api/v1
    private static final List<Function<String, HttpRequestTracker>> trackerFactories = List.of(
        (bucket) -> HttpRequestTracker.get(
            format(Locale.ROOT, "/download/storage/v1/b/%s/o/.+", bucket),
            GoogleCloudStorageOperationsStats::trackGetOperation
        ),

        (bucket) -> HttpRequestTracker.get(
            format(Locale.ROOT, "/storage/v1/b/%s/o/.+", bucket),
            GoogleCloudStorageOperationsStats::trackGetOperation
        ),

        (bucket) -> HttpRequestTracker.get(
            format(Locale.ROOT, "/storage/v1/b/%s/o", bucket),
            GoogleCloudStorageOperationsStats::trackListOperation
        )
    );

    private final GoogleCloudStorageOperationsStats gcsOperationStats;
    private final List<HttpRequestTracker> trackers;

    GoogleCloudStorageHttpStatsCollector(final GoogleCloudStorageOperationsStats gcsOperationStats) {
        this.gcsOperationStats = gcsOperationStats;
        this.trackers = trackerFactories.stream()
            .map(trackerFactory -> trackerFactory.apply(gcsOperationStats.getTrackedBucket()))
            .toList();
    }

    @Override
    public void interceptResponse(final HttpResponse response) {
        // TODO keep track of unsuccessful requests in different entries
        if (response.isSuccessStatusCode() == false) return;

        final HttpRequest request = response.getRequest();
        for (HttpRequestTracker tracker : trackers) {
            if (tracker.track(request, gcsOperationStats)) {
                return;
            }
        }
    }

    /**
     * Http request tracker that allows to track certain HTTP requests based on the following criteria:
     * <ul>
     *     <li>The HTTP request method</li>
     *     <li>An URI path regex expression</li>
     * </ul>
     *
     * The requests that match the previous criteria are tracked using the {@code statsTracker} function.
     */
    private static final class HttpRequestTracker {
        private final String method;
        private final Pattern pathPattern;
        private final Consumer<GoogleCloudStorageOperationsStats> statsTracker;

        private HttpRequestTracker(
            final String method,
            final String pathPattern,
            final Consumer<GoogleCloudStorageOperationsStats> statsTracker
        ) {
            this.method = method;
            this.pathPattern = Pattern.compile(pathPattern);
            this.statsTracker = statsTracker;
        }

        private static HttpRequestTracker get(final String pathPattern, final Consumer<GoogleCloudStorageOperationsStats> statsConsumer) {
            return new HttpRequestTracker("GET", pathPattern, statsConsumer);
        }

        /**
         * Tracks the provided http request if it matches the criteria defined by this tracker.
         *
         * @param httpRequest the http request to be tracked
         * @param stats the operation tracker
         *
         * @return {@code true} if the http request was tracked, {@code false} otherwise.
         */
        private boolean track(final HttpRequest httpRequest, final GoogleCloudStorageOperationsStats stats) {
            if (matchesCriteria(httpRequest) == false) return false;

            statsTracker.accept(stats);
            return true;
        }

        private boolean matchesCriteria(final HttpRequest httpRequest) {
            return method.equalsIgnoreCase(httpRequest.getRequestMethod()) && pathMatches(httpRequest.getUrl());
        }

        private boolean pathMatches(final GenericUrl url) {
            return pathPattern.matcher(url.getRawPath()).matches();
        }
    }
}
