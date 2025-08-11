/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * A service for managing the caching of {@link FieldPermissions} as these may often need to be combined or created and internally they
 * use an {@link org.apache.lucene.util.automaton.Automaton}, which can be costly to create once you account for minimization
 */
public final class FieldPermissionsCache {

    public static final Setting<Long> CACHE_SIZE_SETTING = Setting.longSetting(
        setting("authz.store.roles.field_permissions.cache.max_size_in_bytes"),
        100 * 1024 * 1024,
        -1L,
        Property.NodeScope
    );
    private final Cache<FieldPermissionsDefinition, FieldPermissions> cache;

    public FieldPermissionsCache(Settings settings) {
        this.cache = CacheBuilder.<FieldPermissionsDefinition, FieldPermissions>builder()
            .setMaximumWeight(CACHE_SIZE_SETTING.get(settings))
            .weigher((key, fieldPermissions) -> fieldPermissions.ramBytesUsed())
            .build();
    }

    public Cache.CacheStats getCacheStats() {
        return cache.stats();
    }

    /**
     * Gets a {@link FieldPermissions} instance that corresponds to the granted and denied parameters. The instance may come from the cache
     * or if it gets created, the instance will be cached
     */
    FieldPermissions getFieldPermissions(String[] granted, String[] denied) {
        return getFieldPermissions(new FieldPermissionsDefinition(granted, denied));
    }

    /**
     * Gets a {@link FieldPermissions} instance that corresponds to the granted and denied parameters. The instance may come from the cache
     * or if it gets created, the instance will be cached
     */
    public FieldPermissions getFieldPermissions(FieldPermissionsDefinition fieldPermissionsDefinition) {
        try {
            return cache.computeIfAbsent(
                fieldPermissionsDefinition,
                (key) -> new FieldPermissions(key, FieldPermissions.initializePermittedFieldsAutomaton(key))
            );
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticsearchException) {
                throw (ElasticsearchException) e.getCause();
            } else {
                throw new ElasticsearchSecurityException("unable to compute field permissions", e);
            }
        }
    }

    /**
     * Returns a field permissions object that corresponds to the merging of the given field permissions and caches the instance if one was
     * not found in the cache.
     */
    FieldPermissions getFieldPermissions(Collection<FieldPermissions> fieldPermissionsCollection) {
        Optional<FieldPermissions> allowAllFieldPermissions = fieldPermissionsCollection.stream()
            .filter(((Predicate<FieldPermissions>) (FieldPermissions::hasFieldLevelSecurity)).negate())
            .findFirst();
        return allowAllFieldPermissions.orElseGet(() -> {
            final Set<FieldGrantExcludeGroup> fieldGrantExcludeGroups = new HashSet<>();
            for (FieldPermissions fieldPermissions : fieldPermissionsCollection) {
                final FieldPermissionsDefinition definition = fieldPermissions.getFieldPermissionsDefinition();
                final FieldPermissionsDefinition limitedByDefinition = fieldPermissions.getLimitedByFieldPermissionsDefinition();
                if (definition == null) {
                    throw new IllegalArgumentException("Expected field permission definition, but found null");
                } else if (limitedByDefinition != null) {
                    throw new IllegalArgumentException(
                        "Expected no limited-by field permission definition, but found [" + limitedByDefinition + "]"
                    );
                }
                fieldGrantExcludeGroups.addAll(definition.getFieldGrantExcludeGroups());
            }
            final FieldPermissionsDefinition combined = new FieldPermissionsDefinition(fieldGrantExcludeGroups);
            try {
                return cache.computeIfAbsent(combined, (key) -> {
                    List<Automaton> automatonList = fieldPermissionsCollection.stream()
                        .map(FieldPermissions::getIncludeAutomaton)
                        .collect(Collectors.toList());
                    return new FieldPermissions(key, Automatons.unionAndMinimize(automatonList));
                });
            } catch (ExecutionException e) {
                throw new ElasticsearchException("unable to compute field permissions", e);
            }
        });
    }
}
