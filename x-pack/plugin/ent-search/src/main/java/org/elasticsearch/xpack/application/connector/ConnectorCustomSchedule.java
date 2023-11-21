/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ConnectorCustomSchedule implements Writeable, ToXContentObject {

    private final Map<String, ConfigurationOverride> configurationOverrides;
    private final Boolean enabled;
    private final String interval;
    @Nullable
    private final String lastSynced;
    private final String name;

    private ConnectorCustomSchedule(
        Map<String, ConfigurationOverride> configurationOverrides,
        Boolean enabled,
        String interval,
        String lastSynced,
        String name
    ) {
        this.configurationOverrides = configurationOverrides;
        this.enabled = enabled;
        this.interval = interval;
        this.lastSynced = lastSynced;
        this.name = name;
    }

    public ConnectorCustomSchedule(StreamInput in) throws IOException {
        this.configurationOverrides = in.readMap(StreamInput::readString, ConfigurationOverride::new);
        this.enabled = in.readBoolean();
        this.interval = in.readString();
        this.lastSynced = in.readString();
        this.name = in.readString();
    }

    public static final ParseField CONFIG_OVERRIDES_FIELD = new ParseField("configuration_overrides");
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField INTERVAL_FIELD = new ParseField("interval");
    public static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");

    public static final ParseField NAME_FIELD = new ParseField("name");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ConnectorCustomSchedule, Void> PARSER = new ConstructingObjectParser<>(
        "connector_custom_schedule",
        true,
        args -> new Builder().setConfigurationOverrides((Map<String, ConfigurationOverride>) args[0])
            .setEnabled((Boolean) args[1])
            .setInterval((String) args[2])
            .setLastSynced((String) args[3])
            .setName((String) args[4])
            .build()
    );

    static {
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.map(HashMap::new, ConfigurationOverride::fromXContent),
            CONFIG_OVERRIDES_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareBoolean(constructorArg(), ENABLED_FIELD);
        PARSER.declareString(constructorArg(), INTERVAL_FIELD);
        PARSER.declareString(optionalConstructorArg(), LAST_SYNCED_FIELD);
        PARSER.declareString(constructorArg(), NAME_FIELD);
    }

    public static ConnectorCustomSchedule fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (configurationOverrides != null) {
            builder.field(CONFIG_OVERRIDES_FIELD.getPreferredName(), configurationOverrides);
        }
        if (enabled != null) {
            builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        }
        if (interval != null) {
            builder.field(INTERVAL_FIELD.getPreferredName(), interval);
        }
        if (lastSynced != null) {
            builder.field(LAST_SYNCED_FIELD.getPreferredName(), lastSynced);
        }
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(configurationOverrides, StreamOutput::writeString, StreamOutput::writeGenericValue);
        out.writeBoolean(enabled);
        out.writeString(interval);
        out.writeString(lastSynced);
        out.writeString(name);
    }

    public static class Builder {

        private Map<String, ConfigurationOverride> configurationOverrides;
        private Boolean enabled;
        private String interval;
        private String lastSynced;
        private String name;

        public Builder setConfigurationOverrides(Map<String, ConfigurationOverride> configurationOverrides) {
            this.configurationOverrides = configurationOverrides;
            return this;
        }

        public Builder setEnabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder setInterval(String interval) {
            this.interval = interval;
            return this;
        }

        public Builder setLastSynced(String lastSynced) {
            this.lastSynced = lastSynced;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public ConnectorCustomSchedule build() {
            return new ConnectorCustomSchedule(configurationOverrides, enabled, interval, lastSynced, name);
        }
    }

    public static class ConfigurationOverride implements Writeable, ToXContentObject {
        @Nullable
        private final Integer maxCrawlDepth;
        @Nullable
        private final Boolean sitemapDiscoveryDisabled;
        @Nullable
        private final List<String> domainAllowList;
        @Nullable
        private final List<String> sitemapUrls;
        @Nullable
        private final List<String> seedUrls;

        private ConfigurationOverride(
            Integer maxCrawlDepth,
            Boolean sitemapDiscoveryDisabled,
            List<String> domainAllowList,
            List<String> sitemapUrls,
            List<String> seedUrls
        ) {
            this.maxCrawlDepth = maxCrawlDepth;
            this.sitemapDiscoveryDisabled = sitemapDiscoveryDisabled;
            this.domainAllowList = domainAllowList;
            this.sitemapUrls = sitemapUrls;
            this.seedUrls = seedUrls;
        }

        public ConfigurationOverride(StreamInput in) throws IOException {
            this.maxCrawlDepth = in.readOptionalInt();
            this.sitemapDiscoveryDisabled = in.readOptionalBoolean();
            this.domainAllowList = in.readOptionalStringCollectionAsList();
            this.sitemapUrls = in.readOptionalStringCollectionAsList();
            this.seedUrls = in.readOptionalStringCollectionAsList();
        }

        public static final ParseField MAX_CRAWL_DEPTH_FIELD = new ParseField("max_crawl_depth");
        public static final ParseField SITEMAP_DISCOVERY_DISABLED_FIELD = new ParseField("sitemap_discovery_disabled");
        public static final ParseField DOMAIN_ALLOWLIST_FIELD = new ParseField("domain_allowlist");
        public static final ParseField SITEMAP_URLS_FIELD = new ParseField("sitemap_urls");
        public static final ParseField SEED_URLS_FIELD = new ParseField("seed_urls");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ConfigurationOverride, Void> PARSER = new ConstructingObjectParser<>(
            "configuration_override",
            true,
            args -> new Builder().setMaxCrawlDepth((Integer) args[0])
                .setSitemapDiscoveryDisabled((Boolean) args[1])
                .setDomainAllowList((List<String>) args[2])
                .setSitemapUrls((List<String>) args[3])
                .setSeedUrls((List<String>) args[4])
                .build()
        );

        static {
            PARSER.declareInt(optionalConstructorArg(), MAX_CRAWL_DEPTH_FIELD);
            PARSER.declareBoolean(optionalConstructorArg(), SITEMAP_DISCOVERY_DISABLED_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), DOMAIN_ALLOWLIST_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), SITEMAP_URLS_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), SEED_URLS_FIELD);
        }

        public static ConfigurationOverride fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (maxCrawlDepth != null) {
                builder.field(MAX_CRAWL_DEPTH_FIELD.getPreferredName(), maxCrawlDepth);
            }
            if (sitemapDiscoveryDisabled != null) {
                builder.field(SITEMAP_DISCOVERY_DISABLED_FIELD.getPreferredName(), sitemapDiscoveryDisabled);
            }
            if (domainAllowList != null) {
                builder.field(DOMAIN_ALLOWLIST_FIELD.getPreferredName(), domainAllowList);
            }
            if (sitemapUrls != null) {
                builder.field(SITEMAP_URLS_FIELD.getPreferredName(), sitemapUrls);
            }
            if (seedUrls != null) {
                builder.field(SEED_URLS_FIELD.getPreferredName(), seedUrls);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalInt(maxCrawlDepth);
            out.writeOptionalBoolean(sitemapDiscoveryDisabled);
            out.writeOptionalStringCollection(domainAllowList);
            out.writeOptionalStringCollection(sitemapUrls);
            out.writeOptionalStringCollection(seedUrls);
        }

        public static class Builder {

            private Integer maxCrawlDepth;
            private Boolean sitemapDiscoveryDisabled;
            private List<String> domainAllowList;
            private List<String> sitemapUrls;
            private List<String> seedUrls;

            public Builder setMaxCrawlDepth(Integer maxCrawlDepth) {
                this.maxCrawlDepth = maxCrawlDepth;
                return this;
            }

            public Builder setSitemapDiscoveryDisabled(Boolean sitemapDiscoveryDisabled) {
                this.sitemapDiscoveryDisabled = sitemapDiscoveryDisabled;
                return this;
            }

            public Builder setDomainAllowList(List<String> domainAllowList) {
                this.domainAllowList = domainAllowList;
                return this;
            }

            public Builder setSitemapUrls(List<String> sitemapUrls) {
                this.sitemapUrls = sitemapUrls;
                return this;
            }

            public Builder setSeedUrls(List<String> seedUrls) {
                this.seedUrls = seedUrls;
                return this;
            }

            public ConfigurationOverride build() {
                return new ConfigurationOverride(maxCrawlDepth, sitemapDiscoveryDisabled, domainAllowList, sitemapUrls, seedUrls);
            }
        }
    }
}
