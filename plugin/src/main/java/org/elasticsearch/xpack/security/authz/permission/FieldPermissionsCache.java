/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.Security.setting;

/**
 * A service for managing the caching of {@link FieldPermissions} as these may often need to be combined or created and internally they
 * use an {@link org.apache.lucene.util.automaton.Automaton}, which can be costly to create once you account for minimization
 */
public final class FieldPermissionsCache {

    public static final Setting<Long> CACHE_SIZE_SETTING = Setting.longSetting(
            setting("authz.store.roles.field_permissions.cache.max_size_in_bytes"), 100 * 1024 * 1024, -1L, Property.NodeScope);
    private final Cache<Key, FieldPermissions> cache;

    public FieldPermissionsCache(Settings settings) {
        this.cache = CacheBuilder.<Key, FieldPermissions>builder()
                .setMaximumWeight(CACHE_SIZE_SETTING.get(settings))
                .weigher((key, fieldPermissions) -> fieldPermissions.ramBytesUsed())
                .build();
    }

    /**
     * Gets a {@link FieldPermissions} instance that corresponds to the granted and denied parameters. The instance may come from the cache
     * or if it gets created, the instance will be cached
     */
    FieldPermissions getFieldPermissions(String[] granted, String[] denied) {
        final Set<String> grantedSet;
        if (granted != null) {
            grantedSet = new HashSet<>(granted.length);
            Collections.addAll(grantedSet, granted);
        } else {
            grantedSet = null;
        }

        final Set<String> deniedSet;
        if (denied != null) {
            deniedSet = new HashSet<>(denied.length);
            Collections.addAll(deniedSet, denied);
        } else {
            deniedSet = null;
        }

        return getFieldPermissions(grantedSet, deniedSet);
    }

    /**
     * Gets a {@link FieldPermissions} instance that corresponds to the granted and denied parameters. The instance may come from the cache
     * or if it gets created, the instance will be cached
     */
    public FieldPermissions getFieldPermissions(Set<String> granted, Set<String> denied) {
        Key fpKey = new Key(granted == null ? null : Collections.unmodifiableSet(granted),
                denied == null ? null : Collections.unmodifiableSet(denied));
        try {
            return cache.computeIfAbsent(fpKey,
                    (key) -> new FieldPermissions(key.grantedFields == null ? null : key.grantedFields.toArray(Strings.EMPTY_ARRAY),
                            key.deniedFields == null ? null : key.deniedFields.toArray(Strings.EMPTY_ARRAY)));
        } catch (ExecutionException e) {
            throw new ElasticsearchException("unable to compute field permissions", e);
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
            final Set<String> allowedFields;
            Optional<FieldPermissions> nullAllowedFields = fieldPermissionsCollection.stream()
                    .filter((fieldPermissions) -> fieldPermissions.getGrantedFieldsArray() == null)
                    .findFirst();
            if (nullAllowedFields.isPresent()) {
                allowedFields = null;
            } else {
                allowedFields = fieldPermissionsCollection.stream()
                        .flatMap(fieldPermissions -> Arrays.stream(fieldPermissions.getGrantedFieldsArray()))
                        .collect(Collectors.toSet());
            }

            final Set<String> deniedFields = fieldPermissionsCollection.stream()
                    .filter(fieldPermissions -> fieldPermissions.getDeniedFieldsArray() != null)
                    .flatMap(fieldPermissions -> Arrays.stream(fieldPermissions.getDeniedFieldsArray()))
                    .collect(Collectors.toSet());
            try {
                return cache.computeIfAbsent(new Key(allowedFields, deniedFields),
                        (key) -> {
                    final String[] actualDeniedFields = key.deniedFields == null ? null :
                            computeDeniedFieldsForPermissions(fieldPermissionsCollection, key.deniedFields);
                    return new FieldPermissions(key.grantedFields == null ? null : key.grantedFields.toArray(Strings.EMPTY_ARRAY),
                            actualDeniedFields);
                });
            } catch (ExecutionException e) {
                throw new ElasticsearchException("unable to compute field permissions", e);
            }
        });
    }

    private static String[] computeDeniedFieldsForPermissions(Collection<FieldPermissions> fieldPermissionsCollection,
                                                              Set<String> allDeniedFields) {
        Set<String> allowedDeniedFields = new HashSet<>();
        fieldPermissionsCollection
                .stream()
                .filter(fieldPermissions -> fieldPermissions.getDeniedFieldsArray() != null)
                .forEach((fieldPermissions) -> {
            String[] deniedFieldsForPermission = fieldPermissions.getDeniedFieldsArray();
            fieldPermissionsCollection.forEach((fp) -> {
                if (fp != fieldPermissions) {
                    Arrays.stream(deniedFieldsForPermission).forEach((field) -> {
                        if (fp.grantsAccessTo(field)) {
                            allowedDeniedFields.add(field);
                        }
                    });
                }
            });
        });

        Set<String> difference = Sets.difference(allDeniedFields, allowedDeniedFields);
        if (difference.isEmpty()) {
            return null;
        } else {
            return difference.toArray(Strings.EMPTY_ARRAY);
        }
    }

    private static class Key {

        private final Set<String> grantedFields;
        private final Set<String> deniedFields;

        Key(Set<String> grantedFields, Set<String> deniedFields) {
            this.grantedFields = grantedFields == null ? null : Collections.unmodifiableSet(grantedFields);
            this.deniedFields = deniedFields == null ? null : Collections.unmodifiableSet(deniedFields);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;

            Key key = (Key) o;

            if (grantedFields != null ? !grantedFields.equals(key.grantedFields) : key.grantedFields != null) return false;
            return deniedFields != null ? deniedFields.equals(key.deniedFields) : key.deniedFields == null;
        }

        @Override
        public int hashCode() {
            int result = grantedFields != null ? grantedFields.hashCode() : 0;
            result = 31 * result + (deniedFields != null ? deniedFields.hashCode() : 0);
            return result;
        }
    }
}
