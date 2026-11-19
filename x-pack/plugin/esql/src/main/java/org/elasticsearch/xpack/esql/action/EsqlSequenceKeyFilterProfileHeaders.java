/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Carries the local profile snapshot through the existing async response-header store. */
public final class EsqlSequenceKeyFilterProfileHeaders {
    static final String HEADER = "X-Elastic-Internal-Esql-Sequence-Key-Filter";
    private static final int MAX_HEADER_LENGTH = 4096;
    private static final int MAX_BUCKETS = 24;
    private static final Set<String> COUNTS = Set.of("built_searches", "applied_shard_searches");
    private static final Set<String> BUCKETS = Set.of("coordinator_fallbacks", "rejected_shard_searches", "index_modes");
    private static final Pattern BUCKET_NAME = Pattern.compile("[a-z][a-z0-9_]{0,63}");

    private EsqlSequenceKeyFilterProfileHeaders() {}

    /** Capture before the async completion listener stores its response headers. */
    public static void add(ThreadContext context, EsqlQueryResponse response) {
        if (response.profile() == null || response.getExecutionInfo() == null) {
            return;
        }
        Map<String, Object> snapshot = response.getExecutionInfo().queryProfile().sequenceKeyFilterSnapshot();
        if (snapshot != null) {
            try {
                String value = encode(snapshot);
                if (value.length() <= MAX_HEADER_LENGTH) {
                    context.addResponseHeader(HEADER, value);
                }
            } catch (IOException | RuntimeException e) {
                // The optional profile cannot fail the completed query.
            }
        }
    }

    /** Consume at the final REST boundary, including failure responses. */
    public static ActionListener<EsqlQueryResponse> consume(ThreadContext context, ActionListener<EsqlQueryResponse> listener) {
        return ActionListener.wrap(response -> {
            var headers = context.takeResponseHeaders(HEADER);
            if (headers.size() == 1 && response.profile() != null && response.getExecutionInfo() != null) {
                var profile = response.getExecutionInfo().queryProfile();
                if (profile.sequenceKeyFilterSnapshot() == null) {
                    try {
                        profile.restoreSequenceKeyFilterSnapshot(decode(headers.getFirst()));
                    } catch (IOException | RuntimeException e) {
                        // Malformed or oversized stored diagnostics leave the optional profile field absent.
                    }
                }
            }
            listener.onResponse(response);
        }, e -> {
            context.takeResponseHeaders(HEADER);
            listener.onFailure(e);
        });
    }

    static String encode(Map<String, Object> snapshot) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(snapshot);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(BytesReference.toBytes(BytesReference.bytes(builder)));
        }
    }

    static Map<String, Object> decode(String header) throws IOException {
        if (header.length() > MAX_HEADER_LENGTH) {
            throw invalid();
        }
        byte[] json = Base64.getUrlDecoder().decode(header);
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            if (parser.nextToken() != Token.START_OBJECT) {
                throw invalid();
            }
            Map<String, Object> result = new LinkedHashMap<>();
            for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
                if (token != Token.FIELD_NAME) {
                    throw invalid();
                }
                String name = parser.currentName();
                if (result.containsKey(name)) {
                    throw invalid();
                }
                parser.nextToken();
                if (COUNTS.contains(name)) {
                    result.put(name, count(parser));
                } else if (BUCKETS.contains(name)) {
                    result.put(name, countsByName(parser));
                } else {
                    throw invalid();
                }
            }
            boolean coordinatorOnly = result.keySet().equals(Set.of("built_searches", "coordinator_fallbacks"));
            boolean withShards = result.size() == COUNTS.size() + BUCKETS.size();
            if ((coordinatorOnly == false && withShards == false) || parser.nextToken() != null) {
                throw invalid();
            }
            return Map.copyOf(result);
        }
    }

    private static Map<String, Long> countsByName(XContentParser parser) throws IOException {
        if (parser.currentToken() != Token.START_OBJECT) {
            throw invalid();
        }
        Map<String, Long> result = new LinkedHashMap<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token != Token.FIELD_NAME || result.size() == MAX_BUCKETS) {
                throw invalid();
            }
            String name = parser.currentName();
            if (BUCKET_NAME.matcher(name).matches() == false || result.containsKey(name)) {
                throw invalid();
            }
            parser.nextToken();
            result.put(name, count(parser));
        }
        return Map.copyOf(result);
    }

    private static long count(XContentParser parser) throws IOException {
        if (parser.currentToken() != Token.VALUE_NUMBER) {
            throw invalid();
        }
        var type = parser.numberType();
        if (type != XContentParser.NumberType.INT && type != XContentParser.NumberType.LONG) {
            throw invalid();
        }
        long count = parser.longValue();
        if (count < 0) {
            throw invalid();
        }
        return count;
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("invalid stored SEQUENCE key filter profile");
    }
}
