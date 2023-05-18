/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class WildcardServiceProviderResolver {

    public static final Setting<String> FILE_PATH_SETTING = Setting.simpleString(
        "xpack.idp.sp.wildcard.path",
        "wildcard_services.json",
        Setting.Property.NodeScope
    );

    private class State {
        final Map<String, WildcardServiceProvider> services;
        final Cache<Tuple<String, String>, SamlServiceProvider> cache;

        private State(Map<String, WildcardServiceProvider> services) {
            this.services = services;
            this.cache = ServiceProviderCacheSettings.buildCache(settings);
        }
    }

    private static final Logger logger = LogManager.getLogger(WildcardServiceProviderResolver.class);

    private final Settings settings;
    private final ScriptService scriptService;
    private final SamlServiceProviderFactory serviceProviderFactory;
    private final AtomicReference<State> stateRef;

    WildcardServiceProviderResolver(Settings settings, ScriptService scriptService, SamlServiceProviderFactory serviceProviderFactory) {
        this.settings = settings;
        this.scriptService = scriptService;
        this.serviceProviderFactory = serviceProviderFactory;
        this.stateRef = new AtomicReference<>(new State(Map.of()));
    }

    /**
     * This is implemented as a factory method to facilitate testing - the core resolver just works on InputStreams, this method
     * handles all the Path/ResourceWatcher logic
     */
    public static WildcardServiceProviderResolver create(
        Environment environment,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        SamlServiceProviderFactory spFactory
    ) {
        final Settings settings = environment.settings();
        final Path path = XPackPlugin.resolveConfigFile(environment, FILE_PATH_SETTING.get(environment.settings()));

        logger.info("Loading wildcard services from file [{}]", path.toAbsolutePath());

        final WildcardServiceProviderResolver resolver = new WildcardServiceProviderResolver(settings, scriptService, spFactory);

        if (Files.exists(path)) {
            try {
                resolver.reload(path);
            } catch (IOException e) {
                throw new ElasticsearchException(
                    "File [{}] (from setting [{}]) cannot be loaded",
                    e,
                    path.toAbsolutePath(),
                    FILE_PATH_SETTING.getKey()
                );
            }
        } else if (FILE_PATH_SETTING.exists(environment.settings())) {
            // A file was explicitly configured, but doesn't exist. That's a mistake...
            throw new ElasticsearchException(
                "File [{}] (from setting [{}]) does not exist",
                path.toAbsolutePath(),
                FILE_PATH_SETTING.getKey()
            );
        }

        final FileWatcher fileWatcher = new FileWatcher(path);
        fileWatcher.addListener(new FileChangesListener() {
            @Override
            public void onFileCreated(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileDeleted(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileChanged(Path file) {
                try {
                    resolver.reload(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
        try {
            resourceWatcherService.add(fileWatcher);
        } catch (IOException e) {
            throw new ElasticsearchException(
                "Failed to watch file [{}] (from setting [{}])",
                e,
                path.toAbsolutePath(),
                FILE_PATH_SETTING.getKey()
            );
        }
        return resolver;
    }

    public SamlServiceProvider resolve(String entityId, String acs) {
        final State currentState = stateRef.get();

        Tuple<String, String> cacheKey = new Tuple<>(entityId, acs);
        final SamlServiceProvider cached = currentState.cache.get(cacheKey);
        if (cached != null) {
            logger.trace("Service for [{}] [{}] is cached [{}]", entityId, acs, cached);
            return cached;
        }

        final Map<String, SamlServiceProvider> matches = new HashMap<>();
        currentState.services.forEach((name, wildcard) -> {
            final SamlServiceProviderDocument doc = wildcard.apply(scriptService, entityId, acs);
            if (doc != null) {
                final SamlServiceProvider sp = serviceProviderFactory.buildServiceProvider(doc);
                matches.put(name, sp);
            }
        });

        switch (matches.size()) {
            case 0 -> {
                logger.trace("No wildcard services found for [{}] [{}]", entityId, acs);
                return null;
            }
            case 1 -> {
                final SamlServiceProvider serviceProvider = Iterables.get(matches.values(), 0);
                logger.trace("Found exactly 1 wildcard service for [{}] [{}] - [{}]", entityId, acs, serviceProvider);
                currentState.cache.put(cacheKey, serviceProvider);
                return serviceProvider;
            }
            default -> {
                final String names = Strings.collectionToCommaDelimitedString(matches.keySet());
                logger.warn("Found multiple matching wildcard services for [{}] [{}] - [{}]", entityId, acs, names);
                throw new IllegalStateException(
                    "Found multiple wildcard service providers for entity ID ["
                        + entityId
                        + "] and ACS ["
                        + acs
                        + "] - wildcard service names ["
                        + names
                        + "]"
                );
            }
        }
    }

    // For testing
    Map<String, WildcardServiceProvider> services() {
        return stateRef.get().services;
    }

    // Accessible for testing
    void reload(XContentParser parser) throws IOException {
        final Map<String, WildcardServiceProvider> newServices = Map.copyOf(parse(parser));
        final State oldState = this.stateRef.get();
        if (newServices.equals(oldState.services) == false) {
            // Services have changed
            if (this.stateRef.compareAndSet(oldState, new State(newServices))) {
                logger.info(
                    "Reloaded cached wildcard service providers, new providers [{}]",
                    Strings.collectionToCommaDelimitedString(newServices.keySet())
                );
            } else {
                // some other thread reloaded it
            }
        }
    }

    private void reload(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file); XContentParser parser = buildServicesParser(in)) {
            reload(parser);
        }
    }

    private static XContentParser buildServicesParser(InputStream in) throws IOException {
        return XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, in);
    }

    private static Map<String, WildcardServiceProvider> parse(XContentParser parser) throws IOException {
        final XContentParser.Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        XContentParserUtils.ensureFieldName(parser, parser.nextToken(), Fields.SERVICES.getPreferredName());
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        final Map<String, WildcardServiceProvider> services = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String name = parser.currentName();
            final XContentLocation location = parser.getTokenLocation();
            try {
                services.put(name, WildcardServiceProvider.parse(parser));
            } catch (Exception e) {
                throw new ParsingException(location, "failed to parse wildcard service [{}]", e, name);
            }
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return services;
    }

    public static Collection<? extends Setting<?>> getSettings() {
        return List.of(FILE_PATH_SETTING);
    }

    public interface Fields {
        ParseField SERVICES = new ParseField("services");
    }

}
