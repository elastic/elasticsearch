/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;

import java.util.List;
import java.util.Locale;

/**
 * The tool that sent an ES|QL request when it does not come from Kibana or another Elastic product, for usage telemetry.
 * <p>
 * The value is derived from the {@code x-elastic-product-origin}, {@code x-elastic-client-meta} and {@code User-Agent}
 * request headers, in this order:
 * <ol>
 *     <li>any product origin, or a Kibana User-Agent, is {@link #ELASTIC_PRODUCT}: Kibana uses the JavaScript client and
 *     sends its meta header, so the meta header alone would count Kibana as a direct JavaScript client;</li>
 *     <li>the User-Agent of a known tool ({@link #GRAFANA}, {@link #CURL}), which wins over the meta header of any client
 *     library the tool is built on;</li>
 *     <li>the first language key of the official clients' meta header, e.g. {@code py} in {@code es=9.5.0,py=3.12.1,t=9.4.2};</li>
 *     <li>the official clients' User-Agent, for clients that turned the meta header off;</li>
 *     <li>{@link #OTHER} for everything else, including requests without any of these headers.</li>
 * </ol>
 * Only this closed set leaves the classifier, never a header value or a version. Classification never throws and reads at
 * most {@link #MAX_SCANNED_CHARS} characters of the meta header, so hostile header values stay cheap.
 */
public enum DirectClient {
    PYTHON,
    JAVASCRIPT,
    JAVA,
    GO,
    DOTNET,
    RUBY,
    PHP,
    RUST,
    GRAFANA,
    CURL,
    ELASTIC_PRODUCT,
    OTHER;

    static final String CLIENT_META_HEADER = "x-elastic-client-meta";
    static final String USER_AGENT_HEADER = "User-Agent";

    static final int MAX_SCANNED_CHARS = 512;

    private record Marker(String text, DirectClient client) {}

    private static final List<Marker> TOOL_USER_AGENTS = List.of(
        new Marker("Grafana/", GRAFANA),
        new Marker("Grafana ", GRAFANA),
        new Marker("curl/", CURL)
    );

    private static final List<Marker> META_LANGUAGE_KEYS = List.of(
        new Marker("py", PYTHON),
        new Marker("js", JAVASCRIPT),
        new Marker("jv", JAVA),
        new Marker("go", GO),
        new Marker("net", DOTNET),
        new Marker("rb", RUBY),
        new Marker("php", PHP),
        new Marker("rs", RUST)
    );

    private static final List<Marker> CLIENT_USER_AGENTS = List.of(
        new Marker("elasticsearch-py/", PYTHON),
        new Marker("elasticsearch-js/", JAVASCRIPT),
        new Marker("elastic-java/", JAVA),
        new Marker("elasticsearch-java/", JAVA),
        new Marker("go-elasticsearch/", GO),
        new Marker("elasticsearch-net/", DOTNET),
        new Marker("elasticsearch-ruby/", RUBY),
        new Marker("elasticsearch-php/", PHP),
        new Marker("elasticsearch-rs/", RUST)
    );

    public static DirectClient classify(RestRequest request) {
        return classify(
            request.header(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER),
            request.header(CLIENT_META_HEADER),
            request.header(USER_AGENT_HEADER)
        );
    }

    public static DirectClient classify(@Nullable String productOrigin, @Nullable String clientMeta, @Nullable String userAgent) {
        if (Strings.hasText(productOrigin) || (userAgent != null && userAgent.startsWith("Kibana/"))) {
            return ELASTIC_PRODUCT;
        }
        DirectClient tool = fromUserAgent(userAgent, TOOL_USER_AGENTS);
        if (tool != OTHER) {
            return tool;
        }
        DirectClient language = fromClientMeta(clientMeta);
        if (language != OTHER) {
            return language;
        }
        return fromUserAgent(userAgent, CLIENT_USER_AGENTS);
    }

    private static DirectClient fromUserAgent(@Nullable String userAgent, List<Marker> prefixes) {
        if (userAgent == null) {
            return OTHER;
        }
        for (Marker prefix : prefixes) {
            if (userAgent.startsWith(prefix.text())) {
                return prefix.client();
            }
        }
        return OTHER;
    }

    private static DirectClient fromClientMeta(@Nullable String clientMeta) {
        if (clientMeta == null) {
            return OTHER;
        }
        int end = Math.min(clientMeta.length(), MAX_SCANNED_CHARS);
        int keyStart = 0;
        while (keyStart < end) {
            int pairEnd = clientMeta.indexOf(',', keyStart, end);
            if (pairEnd < 0) {
                pairEnd = end;
            }
            int keyEnd = clientMeta.indexOf('=', keyStart, pairEnd);
            if (keyEnd >= 0) {
                for (Marker key : META_LANGUAGE_KEYS) {
                    if (keyEnd - keyStart == key.text().length() && clientMeta.startsWith(key.text(), keyStart)) {
                        return key.client();
                    }
                }
            }
            keyStart = pairEnd + 1;
        }
        return OTHER;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
