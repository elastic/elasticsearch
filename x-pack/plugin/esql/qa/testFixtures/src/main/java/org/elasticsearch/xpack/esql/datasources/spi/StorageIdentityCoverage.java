/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A census of a provider's {@link StorageIdentity} against the field set its configuration declares.
 * <p>
 * Every provider has per-field tests of the form "two configs differing in this one setting must not share an
 * identity". Those tests are the right assertions and they cannot catch the failure that matters most here: a
 * provider GAINING a setting. Nothing in a list of hand-written cases enumerates the fields they were supposed to
 * cover, so a new setting that never reaches the identity leaves every one of them green while two data sources
 * differing only in that setting share cached bytes.
 * <p>
 * This derives the question from the configuration itself. Each declared setting must reach a component of the
 * identity record — by the camel-case form of its name, or that name with a {@code Digest} suffix, since
 * {@link StorageIdentity#digestSecret} is how a secret is allowed to travel. A setting that genuinely cannot change
 * which bytes are reachable is excluded by name with the reason written down. Add a setting to a provider and this
 * fails until somebody decides which of the two it is.
 * <p>
 * The aliases and exclusions are themselves censused: a name in either map that the configuration does not declare
 * fails too, so a renamed or removed setting cannot leave a stale entry behind that silently excuses its successor.
 * <p>
 * <b>What this reaches, and what it cannot.</b> It applies to a provider whose configuration is a
 * {@link DataSourceConfiguration}, because that is what declares a field set to derive the question from: S3, GCS and
 * Azure. Two implementors of {@link StorageIdentity} are outside it by construction rather than by oversight.
 * {@code HttpConfiguration} is not a {@code DataSourceConfiguration} and declares no field table; its identity folds
 * the custom headers, the only per-data-source credential HTTP sends. {@code FlightStorageProvider} has no
 * configuration at all and declares a zero-component private singleton, which is what {@link StorageIdentity}
 * prescribes for a provider with none. A new provider of either shape gains nothing from this and needs its own
 * argument.
 */
public final class StorageIdentityCoverage {

    private StorageIdentityCoverage() {}

    /**
     * Asserts every setting {@code config} declares reaches {@code identity}.
     *
     * @param aliases    setting name to identity component name, where the two deliberately differ (S3's {@code auth}
     *                   reaches {@code authMode}). An alias is a spelling difference, never a reason to skip a field.
     * @param exclusions setting name to the reason it cannot change which bytes a query can reach. A blank reason is
     *                   itself a failure: an exclusion without one is indistinguishable from an oversight.
     */
    public static void assertEverySettingReachesTheIdentity(
        DataSourceConfiguration config,
        StorageIdentity identity,
        Map<String, String> aliases,
        Map<String, String> exclusions
    ) {
        Set<String> declared = config.fieldNames();
        assertTrue(
            "the configuration declares no settings, so this census would pass without checking anything",
            declared.isEmpty() == false
        );

        RecordComponent[] components = identity.getClass().getRecordComponents();
        if (components == null) {
            fail(
                "["
                    + identity.getClass().getSimpleName()
                    + "] is not a record, so its fields cannot be censused; StorageIdentity documents records as the "
                    + "recommended implementation vehicle"
            );
            return;
        }
        Set<String> present = Arrays.stream(components).map(RecordComponent::getName).collect(Collectors.toCollection(TreeSet::new));

        Set<String> staleAliases = new TreeSet<>(aliases.keySet());
        staleAliases.removeAll(declared);
        assertTrue(
            "aliases name settings the configuration does not declare " + staleAliases + "; a stale alias outlives the field it mapped",
            staleAliases.isEmpty()
        );

        Set<String> staleExclusions = new TreeSet<>(exclusions.keySet());
        staleExclusions.removeAll(declared);
        assertTrue(
            "exclusions name settings the configuration does not declare "
                + staleExclusions
                + "; a stale exclusion silently excuses whatever is named next",
            staleExclusions.isEmpty()
        );

        Set<String> unreached = new TreeSet<>();
        for (String setting : new TreeSet<>(declared)) {
            if (exclusions.containsKey(setting)) {
                assertTrue(
                    "setting [" + setting + "] is excluded from the identity with no reason given",
                    exclusions.get(setting) != null && exclusions.get(setting).isBlank() == false
                );
                continue;
            }
            String component = aliases.getOrDefault(setting, camelCase(setting));
            if (present.contains(component) == false && present.contains(component + "Digest") == false) {
                unreached.add(setting + " (looked for [" + component + "] or [" + component + "Digest])");
            }
        }
        assertTrue(
            "settings declared by ["
                + config.getClass().getSimpleName()
                + "] do not reach ["
                + identity.getClass().getSimpleName()
                + "]: "
                + unreached
                + ". Fold each into the identity, or exclude it by name with the reason it cannot change which bytes "
                + "are reachable. Identity components present: "
                + present,
            unreached.isEmpty()
        );
    }

    /** {@code sts_endpoint} to {@code stsEndpoint}: the record-component spelling of a setting name. */
    private static String camelCase(String settingName) {
        StringBuilder out = new StringBuilder(settingName.length());
        boolean upper = false;
        for (int i = 0; i < settingName.length(); i++) {
            char c = settingName.charAt(i);
            if (c == '_') {
                upper = true;
                continue;
            }
            out.append(upper ? Character.toUpperCase(c) : Character.toLowerCase(c));
            upper = false;
        }
        return out.toString().toLowerCase(Locale.ROOT).isEmpty() ? settingName : out.toString();
    }
}
