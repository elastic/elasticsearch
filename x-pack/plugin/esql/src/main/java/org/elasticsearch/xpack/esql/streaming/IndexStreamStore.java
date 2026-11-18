/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.INDEX;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_COLUMNS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_FAILURE;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_HEADERS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_ID_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_PARTIAL;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_STATUS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TASK;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOOK;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_PAGES;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_ROWS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_WARNINGS;

public class IndexStreamStore implements StreamStorage {

    private static final int METADATA_PAGE_SEQ = 0;

    private final Client client;

    public IndexStreamStore(Client client) {
        this.client = new OriginSettingClient(client, ESQL_ORIGIN);
    }

    @Override
    public void create(StreamMetadata metadata, ActionListener<Void> listener) {
        ActionListener.run(listener, l -> client.index(indexRequest(metadata, OpType.CREATE), l.map(response -> null)));
    }

    @Override
    public void update(StreamMetadata metadata, ActionListener<Void> listener) {
        ActionListener.run(listener, l -> client.index(indexRequest(metadata, OpType.INDEX), l.map(response -> null)));
    }

    @Override
    public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
        client.prepareGet(INDEX, streamId).setRouting(streamId).execute(ActionListener.wrap(response -> {
            if (response.isExists() == false) {
                listener.onResponse(null);
                return;
            }
            ActionListener.completeWith(listener, () -> fromSource(response.getSourceAsMap()));
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    public void appendPage(String streamId, long offset, Page page, ActionListener<Void> listener) {
        // TODO
    }

    @Override
    public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
        // TODO
    }

    @Override
    public void delete(String streamId, ActionListener<Void> listener) {
        // TODO
    }

    private static IndexRequest indexRequest(StreamMetadata metadata, OpType opType) throws IOException {
        return new IndexRequest(INDEX).id(metadata.streamId()).routing(metadata.streamId()).opType(opType).source(toSource(metadata));
    }

    private static XContentBuilder toSource(StreamMetadata metadata) throws IOException {
        XContentBuilder columns = jsonBuilder().startArray();
        for (ColumnInfo column : metadata.columns()) {
            column.toXContent(columns, ToXContent.EMPTY_PARAMS);
        }
        columns.endArray();

        XContentBuilder source = jsonBuilder().startObject()
            .field(STREAM_ID_FIELD, metadata.streamId())
            .field(PAGE_SEQ_FIELD, METADATA_PAGE_SEQ)
            .field(STREAM_TASK, metadata.taskId().toString())
            .field(STREAM_EXPIRATION_TIME_FIELD, metadata.expiration().toEpochMilli())
            .field(STREAM_STATUS, metadata.status().name())
            .field(STREAM_COLUMNS, Strings.toString(columns))
            .field(STREAM_HEADERS, Strings.toString(jsonBuilder().map(metadata.headers())))
            .field(STREAM_WARNINGS, Strings.toString(jsonBuilder().value(metadata.warnings())));
        StreamMetadata.StreamCompletionInfo completion = metadata.completionInfo();
        if (completion != null) {
            source.field(STREAM_PARTIAL, completion.partial()).field(STREAM_TOOK, completion.tookMillis());
            if (completion.pages() != null) {
                source.field(STREAM_TOTAL_PAGES, completion.pages());
            }
            if (completion.rows() != null) {
                source.field(STREAM_TOTAL_ROWS, completion.rows());
            }
            if (completion.failure() != null) {
                source.field(STREAM_FAILURE, failureToJson(completion.failure()));
            }
        }
        return source.endObject();
    }

    private static String failureToJson(StreamMetadata.StreamFailure failure) throws IOException {
        return Strings.toString(
            jsonBuilder().startObject()
                .field("type", failure.type())
                .field("reason", failure.reason())
                .field("status", failure.status().getStatus())
                .endObject()
        );
    }

    private static StreamMetadata fromSource(Map<String, Object> source) throws IOException {
        StreamMetadata.StreamCompletionInfo completion = null;
        if (source.containsKey(STREAM_PARTIAL)) {
            completion = new StreamMetadata.StreamCompletionInfo(
                (Boolean) source.get(STREAM_PARTIAL),
                ((Number) source.get(STREAM_TOOK)).longValue(),
                source.get(STREAM_TOTAL_PAGES) instanceof Number pages ? pages.longValue() : null,
                source.get(STREAM_TOTAL_ROWS) instanceof Number rows ? rows.longValue() : null,
                source.get(STREAM_FAILURE) instanceof String failure ? failureFromJson(failure) : null
            );
        }

        List<ColumnInfo> columns = new ArrayList<>();
        try (XContentParser parser = jsonParser((String) source.get(STREAM_COLUMNS))) {
            while (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                columns.add(ColumnInfoImpl.fromXContent(parser));
            }
        }

        Map<String, String> headers;
        try (XContentParser parser = jsonParser((String) source.get(STREAM_HEADERS))) {
            headers = parser.mapStrings();
        }

        List<String> warnings = new ArrayList<>();
        try (XContentParser parser = jsonParser((String) source.get(STREAM_WARNINGS))) {
            while (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
                warnings.add(parser.text());
            }
        }

        return new StreamMetadata(
            (String) source.get(STREAM_ID_FIELD),
            new TaskId((String) source.get(STREAM_TASK)),
            Instant.ofEpochMilli(((Number) source.get(STREAM_EXPIRATION_TIME_FIELD)).longValue()),
            columns,
            headers,
            warnings,
            StreamMetadata.Status.valueOf((String) source.get(STREAM_STATUS)),
            completion
        );
    }

    private static StreamMetadata.StreamFailure failureFromJson(String json) throws IOException {
        try (XContentParser parser = jsonParser(json)) {
            Map<String, Object> failure = parser.map();
            return new StreamMetadata.StreamFailure(
                (String) failure.get("type"),
                (String) failure.get("reason"),
                RestStatus.fromCode((Integer) failure.get("status"))
            );
        }
    }

    private static XContentParser jsonParser(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json);
        parser.nextToken();
        return parser;
    }
}
