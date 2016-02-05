/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Renders Marvel documents using a XContentBuilder (potentially filtered)
 */
public abstract class AbstractRenderer<T extends MarvelDoc> implements Renderer<T> {

    private static final String[] DEFAULT_FILTERS = {
            Fields.CLUSTER_UUID.underscore().toString(),
            Fields.TIMESTAMP.underscore().toString(),
            Fields.SOURCE_NODE.underscore().toString(),
    };

    private final String[] filters;

    public AbstractRenderer(String[] filters, boolean additive) {
        if (!additive) {
            this.filters = filters;
        } else {
            if (CollectionUtils.isEmpty(filters)) {
                this.filters = DEFAULT_FILTERS;
            } else {
                Set<String> additions = new HashSet<>();
                Collections.addAll(additions, DEFAULT_FILTERS);
                Collections.addAll(additions, filters);
                this.filters = additions.toArray(new String[additions.size()]);
            }
        }
    }

    @Override
    public void render(T marvelDoc, XContentType xContentType, OutputStream os) throws IOException {
        if (marvelDoc != null) {
            try (XContentBuilder builder = new XContentBuilder(xContentType.xContent(), os, filters())) {
                builder.startObject();

                // Add fields common to all Marvel documents
                builder.field(Fields.CLUSTER_UUID, marvelDoc.getClusterUUID());
                DateTime timestampDateTime = new DateTime(marvelDoc.getTimestamp(), DateTimeZone.UTC);
                builder.field(Fields.TIMESTAMP, timestampDateTime.toString());

                MarvelDoc.Node sourceNode = marvelDoc.getSourceNode();
                if (sourceNode != null) {
                    builder.field(Fields.SOURCE_NODE, sourceNode);
                }

                // Render fields specific to the Marvel document
                doRender(marvelDoc, builder, ToXContent.EMPTY_PARAMS);

                builder.endObject();
            }
        }
    }

    protected abstract void doRender(T marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException;

    /**
     * Returns the list of filters used when rendering the document. If null,
     * no filtering is applied.
     */
    protected String[] filters() {
        return filters;
    }

    public static final class Fields {
        public static final XContentBuilderString CLUSTER_UUID = new XContentBuilderString("cluster_uuid");
        public static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        public static final XContentBuilderString SOURCE_NODE = new XContentBuilderString("source_node");
    }
}
