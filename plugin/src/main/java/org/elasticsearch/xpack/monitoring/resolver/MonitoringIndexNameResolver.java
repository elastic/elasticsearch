/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * MonitoringIndexNameResolver are used to resolve the index name, type name
 * and id of a {@link MonitoringDoc}.
 */
public abstract class MonitoringIndexNameResolver<T extends MonitoringDoc> {

    public static final String PREFIX = ".monitoring";
    public static final String DELIMITER = "-";

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

    /**
     * @return the template name that is required by the resolver.
     */
    public abstract String templateName();

    /**
     * @return the template source required by the resolver
     */
    public abstract String template();

    /**
     * @return the filters used when rendering the document.
     * If empty, no filtering is applied.
     */
    public Set<String> filters() {
        // No filtering by default
        return Collections.emptySet();
    }

    protected abstract void buildXContent(T document, XContentBuilder builder, ToXContent.Params params) throws IOException;

    public static final class Fields {
        public static final String CLUSTER_UUID = "cluster_uuid";
        public static final String TIMESTAMP = "timestamp";
        public static final String SOURCE_NODE = "source_node";
    }

    /**
     * Data index name resolvers are used used to index documents in
     * the monitoring data index (.monitoring-data-{VERSION})
     */
    public abstract static class Data<T extends MonitoringDoc> extends MonitoringIndexNameResolver<T> {

        public static final String DATA = "data";

        private final String index;

        public Data() {
            this(MonitoringTemplateUtils.TEMPLATE_VERSION);
        }

        // Used in tests
        protected Data(String version) {
            this.index = String.join(DELIMITER, PREFIX, DATA, version);
        }

        @Override
        public String index(T document) {
            return index;
        }

        @Override
        public String indexPattern() {
            return index;
        }

        @Override
        public String templateName() {
            return String.format(Locale.ROOT, "%s-%s-%s", PREFIX, DATA, MonitoringTemplateUtils.TEMPLATE_VERSION);
        }

        @Override
        public String template() {
            return MonitoringTemplateUtils.loadTemplate(DATA);
        }
    }

    /**
     * Timestamped index name resolvers are used used to index documents in
     * a timestamped index (.monitoring-{ID}-{VERSION}-YYYY.MM.dd)
     */
    public abstract static class Timestamped<T extends MonitoringDoc> extends MonitoringIndexNameResolver<T> {

        public static final Setting<String> INDEX_NAME_TIME_FORMAT_SETTING = new Setting<>("index.name.time_format", "YYYY.MM.dd",
                Function.identity(), Setting.Property.NodeScope);

        private final MonitoredSystem system;
        private final DateTimeFormatter formatter;
        private final String index;

        public Timestamped(MonitoredSystem system, Settings settings) {
            this(system, settings, MonitoringTemplateUtils.TEMPLATE_VERSION);
        }

        // Used in tests
        protected Timestamped(MonitoredSystem system, Settings settings, String version) {
            this.system = system;
            this.index = String.join(DELIMITER, PREFIX, system.getSystem(), version);
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

        @Override
        public String templateName() {
            return String.format(Locale.ROOT, "%s-%s-%s", PREFIX, getId(), MonitoringTemplateUtils.TEMPLATE_VERSION);
        }

        @Override
        public String template() {
            return MonitoringTemplateUtils.loadTemplate(getId());
        }

        String getId() {
            return system.getSystem();
        }
    }
}
