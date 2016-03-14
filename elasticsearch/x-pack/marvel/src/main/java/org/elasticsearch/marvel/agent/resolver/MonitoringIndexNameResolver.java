/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.function.Function;

/**
 * MonitoringIndexNameResolver are used to resolve the index name, type name
 * and id of a {@link MonitoringDoc}.
 */
public abstract class MonitoringIndexNameResolver<T extends MonitoringDoc> {

    public static final String PREFIX = ".monitoring";
    public static final String DELIMITER = "-";

    private final int version;

    protected MonitoringIndexNameResolver(int version) {
        this.version = version;
    }

    /**
     * Returns the name of the index in which the monitoring document must be indexed.
     *
     * @param document the monitoring document
     * @return the name of the index
     */
    public abstract String index(T document);

    /**
     * Returns the generic part of the index name (ie without any dynamic part) that can be
     * used to match indices names.
     *
     * @return the index pattern
     */
    public abstract String indexPattern();

    /**
     * Returns the document type under which the monitoring document must be indexed.
     *
     * @param document the monitoring document
     * @return the type of the document
     */
    public abstract String type(T document);

    /**
     * Returns the document id under which the monitoring document must be indexed.
     *
     * @param document the monitoring document
     * @return the id of the document
     */
    public abstract String id(T document);

    /**
     * Builds the source of the document in a given XContentType
     *
     * @param document     the monitoring document
     * @param xContentType the content type
     * @return the name of the index
     */
    public BytesReference source(T document, XContentType xContentType) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try (XContentBuilder builder = new XContentBuilder(xContentType.xContent(), out, filters())) {
                builder.startObject();

                builder.field(Fields.CLUSTER_UUID, document.getClusterUUID());
                DateTime timestampDateTime = new DateTime(document.getTimestamp(), DateTimeZone.UTC);
                builder.field(Fields.TIMESTAMP, timestampDateTime.toString());

                MonitoringDoc.Node sourceNode = document.getSourceNode();
                if (sourceNode != null) {
                    builder.field(Fields.SOURCE_NODE, sourceNode);
                }

                buildXContent(document, builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }
            return out.bytes();
        }
    }

    int getVersion() {
        return version;
    }

    /**
     * @return the filters used when rendering the document.
     * If null or empty, no filtering is applied.
     */
    public String[] filters() {
        // No filtering by default
        return null;
    }

    protected abstract void buildXContent(T document, XContentBuilder builder, ToXContent.Params params) throws IOException;

    public static final class Fields {
        public static final XContentBuilderString CLUSTER_UUID = new XContentBuilderString("cluster_uuid");
        public static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        public static final XContentBuilderString SOURCE_NODE = new XContentBuilderString("source_node");
    }

    /**
     * Data index name resolvers are used used to index documents in
     * the monitoring data index (.monitoring-data-{VERSION})
     */
    public static abstract class Data<T extends MonitoringDoc> extends MonitoringIndexNameResolver<T> {

        public static final String DATA = "data";

        private final String index;

        public Data(int version) {
            super(version);
            this.index = String.join(DELIMITER, PREFIX, DATA, String.valueOf(version));
        }

        @Override
        public String index(T document) {
            return index;
        }

        @Override
        public String indexPattern() {
            return index;
        }
    }

    /**
     * Timestamped index name resolvers are used used to index documents in
     * a timestamped index (.monitoring-{ID}-{VERSION}-YYYY.MM.dd)
     */
    public static abstract class Timestamped<T extends MonitoringDoc> extends MonitoringIndexNameResolver<T> {

        public static final Setting<String> INDEX_NAME_TIME_FORMAT_SETTING = new Setting<>("index.name.time_format","YYYY.MM.dd",
                Function.identity(), Setting.Property.NodeScope);

        private final MonitoredSystem system;
        private final DateTimeFormatter formatter;
        private final String index;

        public Timestamped(MonitoredSystem system, int version, Settings settings) {
            super(version);
            this.system = system;
            this.index = String.join(DELIMITER, PREFIX, system.getSystem(), String.valueOf(getVersion()));
            String format = INDEX_NAME_TIME_FORMAT_SETTING.get(settings);
            try {
                this.formatter = DateTimeFormat.forPattern(format).withZoneUTC();
            } catch (IllegalArgumentException e) {
                throw new SettingsException("invalid index name time format [" + format + "]", e);
            }
        }

        @Override
        public String index(T document) {
            return String.join(DELIMITER, index, formatter.print(document.getTimestamp()));
        }

        @Override
        public String indexPattern() {
            return String.join(DELIMITER, index, "*");
        }

        @Override
        public String id(T document) {
            // Documents in timestamped indices are usually indexed with auto-generated ids
            return null;
        }

        String getId() {
            return system.getSystem();
        }
    }
}
