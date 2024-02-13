/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.slowlog;

import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.SlowLogMessageFactory;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.security.Security;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class SecuritySlowLogMessageFactory implements SlowLogMessageFactory {
    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));
    private final Security plugin;

    public SecuritySlowLogMessageFactory(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public ESLogMessage indexSlowLogMessage(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {
        Map<String, Object> map = new HashMap<>(plugin.getAuthContextForLogging());
        map.put("elasticsearch.slowlog.message", index);
        map.put("elasticsearch.slowlog.took", TimeValue.timeValueNanos(tookInNanos).toString());
        map.put("elasticsearch.slowlog.took_millis", String.valueOf(TimeUnit.NANOSECONDS.toMillis(tookInNanos)));
        map.put("elasticsearch.slowlog.id", doc.id());
        if (doc.routing() != null) {
            map.put("elasticsearch.slowlog.routing", doc.routing());
        }

        if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
            return new ESLogMessage().withFields(map);
        }
        try {
            String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
            String trim = Strings.cleanTruncate(source, maxSourceCharsToLog).trim();
            StringBuilder sb = new StringBuilder(trim);
            StringBuilders.escapeJson(sb, 0);
            map.put("elasticsearch.slowlog.source", sb.toString());
        } catch (IOException e) {
            StringBuilder sb = new StringBuilder("_failed_to_convert_[" + e.getMessage() + "]");
            StringBuilders.escapeJson(sb, 0);
            map.put("elasticsearch.slowlog.source", sb.toString());
            /*
             * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
             * will be logged at the warn level.
             */
            final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", map.toString());
            throw new UncheckedIOException(message, e);
        }

        return new ESLogMessage().withFields(map);
    }

    @Override
    public ESLogMessage searchSlowLogMessage(SearchContext context, long tookInNanos) {
        Map<String, Object> messageFields = new HashMap<>(plugin.getAuthContextForLogging());
        messageFields.put("elasticsearch.slowlog.message", context.indexShard().shardId());
        messageFields.put("elasticsearch.slowlog.took", TimeValue.timeValueNanos(tookInNanos).toString());
        messageFields.put("elasticsearch.slowlog.took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        if (context.getTotalHits() != null) {
            messageFields.put("elasticsearch.slowlog.total_hits", context.getTotalHits());
        } else {
            messageFields.put("elasticsearch.slowlog.total_hits", "-1");
        }
        messageFields.put(
            "elasticsearch.slowlog.stats",
            escapeJson(ESLogMessage.asJsonArray(context.groupStats() != null ? context.groupStats().stream() : Stream.empty()))
        );
        messageFields.put("elasticsearch.slowlog.search_type", context.searchType());
        messageFields.put("elasticsearch.slowlog.total_shards", context.numberOfShards());

        if (context.request().source() != null) {
            String source = escapeJson(context.request().source().toString(FORMAT_PARAMS));

            messageFields.put("elasticsearch.slowlog.source", source);
        } else {
            messageFields.put("elasticsearch.slowlog.source", "{}");
        }

        messageFields.put("elasticsearch.slowlog.id", context.getTask().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));

        return new ESLogMessage().withFields(messageFields);
    }

    private static String escapeJson(String text) {
        byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
        return new String(sourceEscaped, StandardCharsets.UTF_8);
    }
}
