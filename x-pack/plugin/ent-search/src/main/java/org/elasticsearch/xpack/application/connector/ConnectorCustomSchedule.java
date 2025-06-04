/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ConnectorCustomSchedule implements Writeable, ToXContentObject {

    private final ConfigurationOverrides configurationOverrides;
    private final boolean enabled;
    private final Cron interval;
    @Nullable
    private final Instant lastSynced;
    private final String name;

    /**
     * Constructor for ConnectorCustomSchedule.
     *
     * @param configurationOverrides Configuration overrides {@link ConfigurationOverrides} specifies custom settings overrides.
     * @param enabled                Flag indicating whether the custom schedule is active or not.
     * @param interval               The interval at which the custom schedule runs, specified in a cron-like format.
     * @param lastSynced             The timestamp of the last successful synchronization performed under this custom schedule, if any.
     * @param name                   The name of the custom schedule, used for identification and reference purposes.
     */
    private ConnectorCustomSchedule(
        ConfigurationOverrides configurationOverrides,
        boolean enabled,
        Cron interval,
        Instant lastSynced,
        String name
    ) {
        this.configurationOverrides = Objects.requireNonNull(configurationOverrides, CONFIG_OVERRIDES_FIELD.getPreferredName());
        this.enabled = enabled;
        this.interval = Objects.requireNonNull(interval, INTERVAL_FIELD.getPreferredName());
        this.lastSynced = lastSynced;
        this.name = Objects.requireNonNull(name, NAME_FIELD.getPreferredName());
    }

    public ConnectorCustomSchedule(StreamInput in) throws IOException {
        this.configurationOverrides = new ConfigurationOverrides(in);
        this.enabled = in.readBoolean();
        this.interval = new Cron(in.readString());
        this.lastSynced = in.readOptionalInstant();
        this.name = in.readString();
    }

    private static final ParseField CONFIG_OVERRIDES_FIELD = new ParseField("configuration_overrides");
    private static final ParseField ENABLED_FIELD = new ParseField("enabled");
    private static final ParseField INTERVAL_FIELD = new ParseField("interval");
    private static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");

    private static final ParseField NAME_FIELD = new ParseField("name");

    private static final ConstructingObjectParser<ConnectorCustomSchedule, Void> PARSER = new ConstructingObjectParser<>(
        "connector_custom_schedule",
        true,
        args -> new Builder().setConfigurationOverrides((ConfigurationOverrides) args[0])
            .setEnabled((Boolean) args[1])
            .setInterval(new Cron((String) args[2]))
            .setLastSynced((Instant) args[3])
            .setName((String) args[4])
            .build()
    );

    static {
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> ConfigurationOverrides.fromXContent(parser),
            CONFIG_OVERRIDES_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareBoolean(constructorArg(), ENABLED_FIELD);
        PARSER.declareString(constructorArg(), INTERVAL_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorUtils.parseNullableInstant(p, LAST_SYNCED_FIELD.getPreferredName()),
            LAST_SYNCED_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareString(constructorArg(), NAME_FIELD);
    }

    public static ConnectorCustomSchedule fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static ConnectorCustomSchedule fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return ConnectorCustomSchedule.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse a connector custom schedule.", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(CONFIG_OVERRIDES_FIELD.getPreferredName(), configurationOverrides);
            builder.field(ENABLED_FIELD.getPreferredName(), enabled);
            builder.field(INTERVAL_FIELD.getPreferredName(), interval);
            if (lastSynced != null) {
                builder.field(LAST_SYNCED_FIELD.getPreferredName(), lastSynced);
            }
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(configurationOverrides);
        out.writeBoolean(enabled);
        out.writeString(interval.expression());
        out.writeOptionalInstant(lastSynced);
        out.writeString(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorCustomSchedule that = (ConnectorCustomSchedule) o;
        return enabled == that.enabled
            && Objects.equals(configurationOverrides, that.configurationOverrides)
            && Objects.equals(interval, that.interval)
            && Objects.equals(lastSynced, that.lastSynced)
            && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configurationOverrides, enabled, interval, lastSynced, name);
    }

    public static class Builder {

        private ConfigurationOverrides configurationOverrides;
        private boolean enabled;
        private Cron interval;
        private Instant lastSynced;
        private String name;

        public Builder setConfigurationOverrides(ConfigurationOverrides configurationOverrides) {
            this.configurationOverrides = configurationOverrides;
            return this;
        }

        public Builder setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder setInterval(Cron interval) {
            this.interval = interval;
            return this;
        }

        public Builder setLastSynced(Instant lastSynced) {
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

    public static class ConfigurationOverrides implements Writeable, ToXContentObject {
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

        private ConfigurationOverrides(
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

        public ConfigurationOverrides(StreamInput in) throws IOException {
            this.maxCrawlDepth = in.readOptionalInt();
            this.sitemapDiscoveryDisabled = in.readOptionalBoolean();
            this.domainAllowList = in.readOptionalStringCollectionAsList();
            this.sitemapUrls = in.readOptionalStringCollectionAsList();
            this.seedUrls = in.readOptionalStringCollectionAsList();
        }

        private static final ParseField MAX_CRAWL_DEPTH_FIELD = new ParseField("max_crawl_depth");
        private static final ParseField SITEMAP_DISCOVERY_DISABLED_FIELD = new ParseField("sitemap_discovery_disabled");
        private static final ParseField DOMAIN_ALLOWLIST_FIELD = new ParseField("domain_allowlist");
        private static final ParseField SITEMAP_URLS_FIELD = new ParseField("sitemap_urls");
        private static final ParseField SEED_URLS_FIELD = new ParseField("seed_urls");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ConfigurationOverrides, Void> PARSER = new ConstructingObjectParser<>(
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

        public static ConfigurationOverrides fromXContent(XContentParser parser) throws IOException {
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
                builder.stringListField(DOMAIN_ALLOWLIST_FIELD.getPreferredName(), domainAllowList);
            }
            if (sitemapUrls != null) {
                builder.stringListField(SITEMAP_URLS_FIELD.getPreferredName(), sitemapUrls);
            }
            if (seedUrls != null) {
                builder.stringListField(SEED_URLS_FIELD.getPreferredName(), seedUrls);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConfigurationOverrides that = (ConfigurationOverrides) o;
            return Objects.equals(maxCrawlDepth, that.maxCrawlDepth)
                && Objects.equals(sitemapDiscoveryDisabled, that.sitemapDiscoveryDisabled)
                && Objects.equals(domainAllowList, that.domainAllowList)
                && Objects.equals(sitemapUrls, that.sitemapUrls)
                && Objects.equals(seedUrls, that.seedUrls);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxCrawlDepth, sitemapDiscoveryDisabled, domainAllowList, sitemapUrls, seedUrls);
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

            public ConfigurationOverrides build() {
                return new ConfigurationOverrides(maxCrawlDepth, sitemapDiscoveryDisabled, domainAllowList, sitemapUrls, seedUrls);
            }
        }
    }
}
