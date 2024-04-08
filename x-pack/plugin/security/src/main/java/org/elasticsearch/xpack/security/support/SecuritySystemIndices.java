/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_VERSION_STRING;

/**
 * Responsible for handling system indices for the Security plugin
 */
public class SecuritySystemIndices {

    public static final int INTERNAL_MAIN_INDEX_FORMAT = 6;
    private static final int INTERNAL_MAIN_INDEX_MAPPINGS_FORMAT = 1;
    private static final int INTERNAL_TOKENS_INDEX_FORMAT = 7;
    private static final int INTERNAL_TOKENS_INDEX_MAPPINGS_FORMAT = 1;
    private static final int INTERNAL_PROFILE_INDEX_FORMAT = 8;
    private static final int INTERNAL_PROFILE_INDEX_MAPPINGS_FORMAT = 2;

    public static final String SECURITY_MAIN_ALIAS = ".security";
    private static final String MAIN_INDEX_CONCRETE_NAME = ".security-7";
    public static final String SECURITY_TOKENS_ALIAS = ".security-tokens";
    private static final String TOKENS_INDEX_CONCRETE_NAME = ".security-tokens-7";

    public static final String INTERNAL_SECURITY_PROFILE_INDEX_8 = ".security-profile-8";
    public static final String SECURITY_PROFILE_ALIAS = ".security-profile";
    public static final Version VERSION_SECURITY_PROFILE_ORIGIN = Version.V_8_3_0;
    public static final NodeFeature SECURITY_PROFILE_ORIGIN_FEATURE = new NodeFeature("security.security_profile_origin");

    private static final Logger logger = LogManager.getLogger(SecuritySystemIndices.class);

    private final SystemIndexDescriptor mainDescriptor;
    private final SystemIndexDescriptor tokenDescriptor;
    private final SystemIndexDescriptor profileDescriptor;
    private final AtomicBoolean initialized;
    private SecurityIndexManager mainIndexManager;
    private SecurityIndexManager tokenIndexManager;
    private SecurityIndexManager profileIndexManager;

    public SecuritySystemIndices(Settings settings) {
        this.mainDescriptor = getSecurityMainIndexDescriptor();
        this.tokenDescriptor = getSecurityTokenIndexDescriptor();
        this.profileDescriptor = getSecurityProfileIndexDescriptor(settings);
        this.initialized = new AtomicBoolean(false);
        this.mainIndexManager = null;
        this.tokenIndexManager = null;
        this.profileIndexManager = null;
    }

    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return List.of(mainDescriptor, tokenDescriptor, profileDescriptor);
    }

    public void init(Client client, ClusterService clusterService) {
        if (this.initialized.compareAndSet(false, true) == false) {
            throw new IllegalStateException("Already initialized");
        }
        this.mainIndexManager = SecurityIndexManager.buildSecurityIndexManager(client, clusterService, mainDescriptor);
        this.tokenIndexManager = SecurityIndexManager.buildSecurityIndexManager(client, clusterService, tokenDescriptor);
        this.profileIndexManager = SecurityIndexManager.buildSecurityIndexManager(client, clusterService, profileDescriptor);
    }

    public SecurityIndexManager getMainIndexManager() {
        checkInitialized();
        return this.mainIndexManager;
    }

    public SecurityIndexManager getTokenIndexManager() {
        checkInitialized();
        return this.tokenIndexManager;
    }

    public SecurityIndexManager getProfileIndexManager() {
        return profileIndexManager;
    }

    private void checkInitialized() {
        if (this.initialized.get() == false) {
            String message = "Attempt access " + getClass().getSimpleName() + " before it is initialized";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    private SystemIndexDescriptor getSecurityMainIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            // This can't just be `.security-*` because that would overlap with the tokens index pattern
            .setIndexPattern(".security-[0-9]+*")
            .setPrimaryIndex(MAIN_INDEX_CONCRETE_NAME)
            .setDescription("Contains Security configuration")
            .setMappings(getMainIndexMappings())
            .setSettings(getMainIndexSettings())
            .setAliasName(SECURITY_MAIN_ALIAS)
            .setIndexFormat(INTERNAL_MAIN_INDEX_FORMAT)
            .setVersionMetaKey("security-version")
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static Settings getMainIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 1000)
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), INTERNAL_MAIN_INDEX_FORMAT)
            .put("analysis.filter.email.type", "pattern_capture")
            .put("analysis.filter.email.preserve_original", true)
            .putList("analysis.filter.email.patterns", List.of("([^@]+)", "(\\p{L}+)", "(\\d+)", "@(.+)"))
            .put("analysis.analyzer.email.tokenizer", "uax_url_email")
            .putList("analysis.analyzer.email.filter", List.of("email", "lowercase", "unique"))
            .build();
    }

    private XContentBuilder getMainIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(SECURITY_VERSION_STRING, Version.CURRENT.toString());
                builder.field(SystemIndexDescriptor.VERSION_META_KEY, INTERNAL_MAIN_INDEX_MAPPINGS_FORMAT);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("username");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("roles");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("role_templates");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("template");
                            builder.field("type", "text");
                            builder.endObject();

                            builder.startObject("format");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("password");
                    builder.field("type", "keyword");
                    builder.field("index", false);
                    builder.field("doc_values", false);
                    builder.endObject();

                    builder.startObject("full_name");
                    builder.field("type", "text");
                    builder.endObject();

                    builder.startObject("email");
                    builder.field("type", "text");
                    builder.field("analyzer", "email");
                    builder.endObject();

                    builder.startObject("metadata");
                    builder.field("type", "object");
                    builder.field("dynamic", false);
                    builder.endObject();

                    builder.startObject("metadata_flattened");
                    builder.field("type", "flattened");
                    builder.endObject();

                    builder.startObject("enabled");
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject("cluster");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("indices");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("field_security");
                            {
                                builder.startObject("properties");
                                {
                                    builder.startObject("grant");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("except");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("names");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("privileges");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("query");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("allow_restricted_indices");
                            builder.field("type", "boolean");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("remote_indices");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("field_security");
                            {
                                builder.startObject("properties");
                                {
                                    builder.startObject("grant");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("except");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("names");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("privileges");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("query");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("allow_restricted_indices");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("clusters");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("applications");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("application");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("privileges");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("resources");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("application");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("global");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("application");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("manage");
                                    {
                                        builder.field("type", "object");
                                        builder.startObject("properties");
                                        {
                                            builder.startObject("applications");
                                            builder.field("type", "keyword");
                                            builder.endObject();
                                        }
                                        builder.endObject();
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                            builder.startObject("profile");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("write");
                                    {
                                        builder.field("type", "object");
                                        builder.startObject("properties");
                                        {
                                            builder.startObject("applications");
                                            builder.field("type", "keyword");
                                            builder.endObject();
                                        }
                                        builder.endObject();
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("name");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("run_as");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("doc_type");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("type");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("actions");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("expiration_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("creation_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("invalidation_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("api_key_hash");
                    builder.field("type", "keyword");
                    builder.field("index", false);
                    builder.field("doc_values", false);
                    builder.endObject();

                    builder.startObject("api_key_invalidated");
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject("role_descriptors");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();

                    builder.startObject("limited_by_role_descriptors");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();

                    builder.startObject("version");
                    builder.field("type", "integer");
                    builder.endObject();

                    builder.startObject("creator");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("principal");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("full_name");
                            builder.field("type", "text");
                            builder.endObject();

                            builder.startObject("email");
                            builder.field("type", "text");
                            builder.field("analyzer", "email");
                            builder.endObject();

                            builder.startObject("metadata");
                            builder.field("type", "object");
                            builder.field("dynamic", false);
                            builder.endObject();

                            builder.startObject("realm");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("realm_type");
                            builder.field("type", "keyword");
                            builder.endObject();

                            defineRealmDomain(builder, "realm_domain");
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("rules");
                    builder.field("type", "object");
                    builder.field("dynamic", false);
                    builder.endObject();

                    builder.startObject("refresh_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("token");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("refreshed");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("refresh_time");
                            builder.field("type", "date");
                            builder.field("format", "epoch_millis");
                            builder.endObject();

                            builder.startObject("superseding");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("encrypted_tokens");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_iv");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_salt");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("client");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("type");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("user");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("realm");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("access_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("user_token");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("id");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("expiration_time");
                                    builder.field("type", "date");
                                    builder.field("format", "epoch_millis");
                                    builder.endObject();

                                    builder.startObject("version");
                                    builder.field("type", "integer");
                                    builder.endObject();

                                    builder.startObject("metadata");
                                    builder.field("type", "object");
                                    builder.field("dynamic", false);
                                    builder.endObject();

                                    builder.startObject("authentication");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("realm");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + MAIN_INDEX_CONCRETE_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + MAIN_INDEX_CONCRETE_NAME + " index mappings", e);
        }
    }

    private static SystemIndexDescriptor getSecurityTokenIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".security-tokens-[0-9]+*")
            .setPrimaryIndex(TOKENS_INDEX_CONCRETE_NAME)
            .setDescription("Contains auth token data")
            .setMappings(getTokenIndexMappings())
            .setSettings(getTokenIndexSettings())
            .setAliasName(SECURITY_TOKENS_ALIAS)
            .setIndexFormat(INTERNAL_TOKENS_INDEX_FORMAT)
            .setVersionMetaKey(SECURITY_VERSION_STRING)
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static Settings getTokenIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 1000)
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), INTERNAL_TOKENS_INDEX_FORMAT)
            .build();
    }

    private static XContentBuilder getTokenIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(SECURITY_VERSION_STRING, Version.CURRENT);
                builder.field(SystemIndexDescriptor.VERSION_META_KEY, INTERNAL_TOKENS_INDEX_MAPPINGS_FORMAT);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("doc_type");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("creation_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("refresh_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("token");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("refreshed");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("refresh_time");
                            builder.field("type", "date");
                            builder.field("format", "epoch_millis");
                            builder.endObject();

                            builder.startObject("superseding");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("encrypted_tokens");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_iv");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_salt");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("client");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("type");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("user");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("realm");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    defineRealmDomain(builder, "realm_domain");

                                    builder.startObject("authentication").field("type", "binary").endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("access_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("user_token");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("id");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("expiration_time");
                                    builder.field("type", "date");
                                    builder.field("format", "epoch_millis");
                                    builder.endObject();

                                    builder.startObject("version");
                                    builder.field("type", "integer");
                                    builder.endObject();

                                    builder.startObject("metadata");
                                    builder.field("type", "object");
                                    builder.field("dynamic", false);
                                    builder.endObject();

                                    builder.startObject("authentication");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("token");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("realm");
                            builder.field("type", "keyword");
                            builder.endObject();

                            defineRealmDomain(builder, "realm_domain");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + TOKENS_INDEX_CONCRETE_NAME + " index mappings", e);
        }
    }

    private SystemIndexDescriptor getSecurityProfileIndexDescriptor(Settings settings) {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".security-profile-[0-9]+*")
            .setPrimaryIndex(INTERNAL_SECURITY_PROFILE_INDEX_8)
            .setDescription("Contains user profile documents")
            .setMappings(getProfileIndexMappings(INTERNAL_PROFILE_INDEX_MAPPINGS_FORMAT))
            .setSettings(getProfileIndexSettings(settings))
            .setAliasName(SECURITY_PROFILE_ALIAS)
            .setIndexFormat(INTERNAL_PROFILE_INDEX_FORMAT)
            .setVersionMetaKey(SECURITY_VERSION_STRING)
            .setOrigin(SECURITY_PROFILE_ORIGIN) // new origin since 8.3
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .setMinimumNodeVersion(VERSION_SECURITY_PROFILE_ORIGIN)
            .setPriorSystemIndexDescriptors(
                List.of(
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(".security-profile-[0-9]+*")
                        .setPrimaryIndex(INTERNAL_SECURITY_PROFILE_INDEX_8)
                        .setDescription("Contains user profile documents")
                        .setMappings(getProfileIndexMappings(INTERNAL_PROFILE_INDEX_MAPPINGS_FORMAT - 1))
                        .setSettings(getProfileIndexSettings(settings))
                        .setAliasName(SECURITY_PROFILE_ALIAS)
                        .setIndexFormat(INTERNAL_PROFILE_INDEX_FORMAT)
                        .setVersionMetaKey(SECURITY_VERSION_STRING)
                        .setOrigin(SECURITY_ORIGIN)
                        .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
                        .build()
                )
            )
            .build();
    }

    private static Settings getProfileIndexSettings(Settings settings) {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 1000)
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), INTERNAL_PROFILE_INDEX_FORMAT)
            .put("analysis.filter.email.type", "pattern_capture")
            .put("analysis.filter.email.preserve_original", true)
            .putList("analysis.filter.email.patterns", List.of("([^@]+)", "(\\p{L}+)", "(\\d+)", "@(.+)"))
            .put("analysis.analyzer.email.tokenizer", "uax_url_email")
            .putList("analysis.analyzer.email.filter", List.of("email", "lowercase", "unique"));
        if (DiscoveryNode.isStateless(settings)) {
            // The profiles functionality is intrinsically related to Kibana. Only Kibana uses this index (via dedicated APIs).
            // Since the regular ".kibana" index is marked "fast_refresh", we opt to mark the user profiles index as "fast_refresh" too.
            // This way the profiles index has the same availability and latency characteristics as the regular ".kibana" index, so APIs
            // touching either of the two indices are more predictable.
            settingsBuilder.put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true);
        }
        return settingsBuilder.build();
    }

    private XContentBuilder getProfileIndexMappings(int mappingsVersion) {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(SECURITY_VERSION_STRING, Version.CURRENT.toString());
                builder.field(SystemIndexDescriptor.VERSION_META_KEY, mappingsVersion);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("user_profile");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("uid");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("enabled");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("user");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("username");
                                    builder.field("type", "search_as_you_type");
                                    builder.startObject("fields");
                                    {
                                        builder.startObject("keyword");
                                        builder.field("type", "keyword");
                                        builder.endObject();
                                    }
                                    builder.endObject();
                                    builder.endObject();

                                    builder.startObject("roles");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("realm");
                                    {
                                        builder.field("type", "object");
                                        builder.startObject("properties");
                                        {
                                            builder.startObject("name");
                                            builder.field("type", "keyword");
                                            builder.endObject();

                                            builder.startObject("type");
                                            builder.field("type", "keyword");
                                            builder.endObject();

                                            defineRealmDomain(builder, "domain");

                                            builder.startObject("node_name");
                                            builder.field("type", "keyword");
                                            builder.endObject();
                                        }
                                        builder.endObject();
                                    }
                                    builder.endObject();

                                    builder.startObject("email");
                                    builder.field("type", "text");
                                    builder.field("analyzer", "email");
                                    builder.endObject();

                                    builder.startObject("full_name");
                                    builder.field("type", "search_as_you_type");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("last_synchronized");
                            builder.field("type", "date");
                            builder.field("format", "epoch_millis");
                            builder.endObject();

                            // Searchable application specific data
                            builder.startObject("labels");
                            builder.field("type", "flattened");
                            builder.endObject();

                            // Non-searchable application specific data, retrievable but not searchable
                            builder.startObject("application_data");
                            {
                                builder.field("type", "object");
                                builder.field("enabled", false);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build profile index mappings", e);
            throw new UncheckedIOException("Failed to build profile index mappings", e);
        }
    }

    private static void defineRealmDomain(XContentBuilder builder, String fieldName) throws IOException {
        builder.startObject(fieldName);
        {
            builder.field("type", "object");
            builder.startObject("properties");
            {
                builder.startObject("name");
                builder.field("type", "keyword");
                builder.endObject();

                builder.startObject("realms");
                {
                    builder.field("type", "nested");
                    builder.startObject("properties");
                    {
                        builder.startObject("name");
                        builder.field("type", "keyword");
                        builder.endObject();

                        builder.startObject("type");
                        builder.field("type", "keyword");
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
    }

}
