/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.glob.ExclusionConfig;
import org.elasticsearch.xpack.esql.datasources.glob.FileOrderConfig;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Every resolver-level setting must be accounted for by the dataset schema key, one way or another. A setting that
 * changes the inferred schema and is accounted for nowhere is the one way this cache serves a WRONG schema rather than
 * a slow one: two datasets differing only in it would share an entry.
 * <p>
 * Each key is classified by NAME, not by the set it came from. Classifying a whole source set at once — "every split
 * setting is inert" — would let a new key added to that set inherit the verdict unexamined. Instead the classified keys
 * must equal {@link FileSourceFactory#COORDINATOR_KEYS} exactly: a new key fails this test until someone decides where
 * it belongs, and a removed one fails it too, so the classification cannot go stale.
 * <p>
 * Modelled on {@code StatsInvalidationScopeTests}, which does the same for the {@code _stats.*} vocabulary.
 */
public class DatasetSchemaKeysScopeTests extends ESTestCase {

    /** In the key's settings: they change the schema whatever the reader. Must equal the production set. */
    private static final Set<String> IN_THE_KEY = Set.of(
        ExternalSourceResolver.CONFIG_SCHEMA_RESOLUTION,
        PartitionConfig.CONFIG_PARTITIONING_DETECTION,
        PartitionConfig.CONFIG_PARTITIONING_PATH
    );

    /** Accounted for by each reader's own declaration: they reach some readers' inference and not others'. */
    private static final Set<String> READER_DECIDES = ErrorPolicy.CONFIG_KEYS;

    /** Select the reader, whose name is the key's format type. */
    private static final Set<String> CAPTURED_BY_FORMAT = Set.of(FileSourceFactory.CONFIG_FORMAT, FormatNameResolver.CONFIG_READER);

    /**
     * Decide which files are listed, or which is listed first. Either way they change the files the identity is taken
     * from, so the identity already reflects them: a different set changes the set fingerprint, a different head the
     * anchor's.
     */
    private static final Set<String> CAPTURED_BY_FILE_IDENTITY = Set.of(
        ExclusionConfig.CONFIG_FILE_EXCLUSIONS,
        FileOrderConfig.CONFIG_FILE_SORT_BY,
        FileOrderConfig.CONFIG_FILE_ORDER
    );

    /** Carries the data source's connection settings; the key takes endpoint and region from it, never credentials. */
    private static final Set<String> CAPTURED_BY_LOCATION = Set.of(ExternalSourceResolver.DATASOURCE_CONFIG_KEY);

    /**
     * Cannot change what is inferred. Split sizing shapes how files are read, not their schema. {@code hive_partitioning}
     * is a deprecated setting nothing reads, and {@code partition_sample_size} only bounds a listing.
     */
    private static final Set<String> INERT = Set.of(
        FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE,
        FileSplitProvider.CONFIG_SPLIT_PROBE_WINDOW,
        FileSplitProvider.CONFIG_MAX_SPLIT_PROBES,
        PartitionConfig.CONFIG_PARTITIONING_HIVE,
        PartitionConfig.CONFIG_PARTITION_SAMPLE_SIZE
    );

    private static Map<String, Set<String>> classification() {
        Map<String, Set<String>> byClass = new HashMap<>();
        byClass.put("IN_THE_KEY", IN_THE_KEY);
        byClass.put("READER_DECIDES", READER_DECIDES);
        byClass.put("CAPTURED_BY_FORMAT", CAPTURED_BY_FORMAT);
        byClass.put("CAPTURED_BY_FILE_IDENTITY", CAPTURED_BY_FILE_IDENTITY);
        byClass.put("CAPTURED_BY_LOCATION", CAPTURED_BY_LOCATION);
        byClass.put("INERT", INERT);
        return byClass;
    }

    public void testEveryResolverSettingIsClassifiedExactlyOnce() {
        Set<String> classified = new HashSet<>();
        for (Map.Entry<String, Set<String>> bucket : classification().entrySet()) {
            for (String key : bucket.getValue()) {
                assertTrue("[" + key + "] is classified twice; the second is " + bucket.getKey(), classified.add(key));
            }
        }
        Set<String> unclassified = new HashSet<>(FileSourceFactory.COORDINATOR_KEYS);
        unclassified.removeAll(classified);
        assertEquals(
            "a resolver setting nothing accounts for: decide whether it changes the inferred schema and classify it here",
            Set.of(),
            unclassified
        );
        Set<String> stale = new HashSet<>(classified);
        stale.removeAll(FileSourceFactory.COORDINATOR_KEYS);
        assertEquals("classified but no longer a resolver setting: remove it here", Set.of(), stale);
    }

    public void testTheKeysSettingsAreExactlyTheOnesClassifiedAsInTheKey() {
        assertEquals(IN_THE_KEY, DatasetSchemaKeys.RESOLVER_SCHEMA_KEYS);
    }
}
