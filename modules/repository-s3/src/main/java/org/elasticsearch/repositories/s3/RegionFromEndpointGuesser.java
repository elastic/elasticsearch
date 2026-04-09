/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple mapping from S3 endpoint hostnames to AWS region names, in case the user does not specify a region. This allows Elasticsearch to
 * guess an appropriate AWS region name, and keep working, if a user does not specify one. This is a best-effort attempt for backwards
 * compatibility: AWS SDK V1 would extrapolate the correct region but V2 considers it a fatal error not to be told the region name
 * explicitly.
 * <p>
 * The mapping is loaded from the JAR resource named {@code regions_by_endpoint.txt}.
 * </p>
 */
class RegionFromEndpointGuesser {
    private static final Map<String, String> regionsByEndpoint;

    static {
        try (
            var resourceStream = readFromJarResourceUrl(RegionFromEndpointGuesser.class.getResource("regions_by_endpoint.txt"));
            var reader = new BufferedReader(new InputStreamReader(resourceStream, StandardCharsets.UTF_8))
        ) {
            final var builder = new HashMap<String, String>();
            while (true) {
                final var line = reader.readLine();
                if (line == null) {
                    break;
                }
                final var parts = line.split(" +");
                if (parts.length != 2) {
                    throw new IllegalStateException("invalid regions_by_endpoint.txt line: " + line);
                }
                builder.put(parts[1], parts[0]);
            }
            regionsByEndpoint = Map.copyOf(builder);
        } catch (Exception e) {
            assert false : e;
            throw new IllegalStateException("could not read regions_by_endpoint.txt", e);
        }
    }

    @SuppressForbidden(reason = "reads resource from jar")
    private static InputStream readFromJarResourceUrl(URL source) throws IOException {
        if (source == null) {
            throw new FileNotFoundException("links resource not found at [" + source + "]");
        }
        return source.openStream();
    }

    /**
     * @return a guess at the region name for the given S3 endpoint, or {@code null} if the endpoint is not recognised.
     */
    @Nullable
    static String guessRegion(@Nullable String endpoint) {
        if (endpoint == null) {
            return null;
        }

        if (endpoint.startsWith("https://")) {
            endpoint = endpoint.substring("https://".length());
        }

        return regionsByEndpoint.get(endpoint);
    }

}
