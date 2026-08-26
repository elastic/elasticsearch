/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pins {@link FileSourceFactory#COORDINATOR_KEYS} membership against the keys other code in this
 * factory actually consumes. A missing entry would cause a real configuration option (e.g.
 * {@code error_mode}, {@code target_split_size}) to be flagged as unknown for every user.
 *
 * <p>Two layers of safety:
 * <ul>
 *   <li>Per-component {@code CONFIG_KEYS} sets must each appear in the union (this class).
 *   <li>Each component's own {@code static final String CONFIG_*} string constants must match
 *       its {@code CONFIG_KEYS} set bidirectionally (reflection check below).
 *       Catches both 'added a new {@code CONFIG_X} constant but forgot to register' and
 *       'added an entry to {@code CONFIG_KEYS} that no constant declares (dead entry)'.
 * </ul>
 *
 * <p><b>Naming contract:</b> the bidirectional reflection check enumerates fields whose name starts
 * with {@code CONFIG_}. User-facing configuration keys MUST follow this prefix; otherwise the test
 * silently misses them (a {@code MY_OPTION_KEY}-style suffix would not be picked up). Internal
 * marker strings unrelated to user-facing config (split markers, file metadata keys, etc.) should
 * use a different naming pattern to keep the contract clear.
 *
 * <p>The generic validator contract lives in {@code ConfigKeyValidatorTests}.
 */
public class FileSourceFactoryValidationTests extends ESTestCase {

    public void testCoordinatorKeysIncludesFormatOverride() {
        assertTrue(FileSourceFactory.COORDINATOR_KEYS.contains(FileSourceFactory.CONFIG_FORMAT));
    }

    public void testCoordinatorKeysIncludesReaderOverride() {
        assertTrue(
            "FormatNameResolver.CONFIG_READER must be a coordinator key (the format-name resolver reads it)",
            FileSourceFactory.COORDINATOR_KEYS.contains(FormatNameResolver.CONFIG_READER)
        );
    }

    /**
     * Pins {@code StorageProviderRegistry.FRAMEWORK_KEYS ⊆ FileSourceFactory.COORDINATOR_KEYS}.
     * Both sets describe coordinator/framework-level config keys; if a future change adds a key
     * to FRAMEWORK_KEYS (for the storage-side strip step) but forgets the coordinator side, the
     * validator silently rejects every query that uses it. This unit-time check catches that
     * drift before it ships.
     */
    public void testFrameworkKeysAreSubsetOfCoordinatorKeys() {
        Set<String> missing = new TreeSet<>(StorageProviderRegistry.FRAMEWORK_KEYS);
        missing.removeAll(FileSourceFactory.COORDINATOR_KEYS);
        assertTrue("FRAMEWORK_KEYS not in COORDINATOR_KEYS: " + missing, missing.isEmpty());
    }

    public void testCoordinatorKeysIncludesAllErrorPolicyKeys() {
        for (String key : ErrorPolicy.CONFIG_KEYS) {
            assertTrue("ErrorPolicy key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }

    public void testCoordinatorKeysIncludesAllFileSplitProviderKeys() {
        for (String key : FileSplitProvider.CONFIG_KEYS) {
            assertTrue("FileSplitProvider key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }

    public void testErrorPolicyConfigKeysMatchConstants() {
        assertConfigKeysMatchConstants(ErrorPolicy.class, ErrorPolicy.CONFIG_KEYS);
    }

    public void testFileSplitProviderConfigKeysMatchConstants() {
        assertConfigKeysMatchConstants(FileSplitProvider.class, FileSplitProvider.CONFIG_KEYS);
    }

    public void testCoordinatorKeysIncludesAllPartitionConfigKeys() {
        for (String key : PartitionConfig.CONFIG_KEYS) {
            assertTrue("PartitionConfig key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }

    public void testPartitionConfigKeysMatchConstants() {
        assertConfigKeysMatchConstants(PartitionConfig.class, PartitionConfig.CONFIG_KEYS);
    }

    /**
     * Pins the dataset CRUD vocabulary against the query path: the coordinator-level data-shape keys a
     * dataset accepts must be exactly {@code COORDINATOR_KEYS} minus the EXTERNAL-only allowlist
     * ({@code reader}) and the internal {@code _datasource} envelope. {@code format} is a first-class
     * dataset setting and so must be present in {@code COORDINATOR_DATASET_KEYS}. If a future change
     * adds a coordinator key without either exposing it on the dataset or allowlisting it as
     * EXTERNAL-only, this fails, so a real option cannot silently become EXTERNAL-only (or vice versa).
     */
    public void testDatasetCoordinatorKeysTrackCoordinatorKeys() {
        Set<String> expected = new TreeSet<>(FileSourceFactory.COORDINATOR_KEYS);
        expected.removeAll(FileSourceFactory.EXTERNAL_ONLY_KEYS);
        expected.remove(ExternalSourceResolver.DATASOURCE_CONFIG_KEY);
        assertEquals(
            "dataset coordinator keys must equal COORDINATOR_KEYS minus the EXTERNAL-only allowlist and the internal " + "_datasource key",
            expected,
            new TreeSet<>(FileDataSourceValidator.COORDINATOR_DATASET_KEYS)
        );
    }

    public void testExternalOnlyKeysIsExactlyReader() {
        assertEquals(Set.of(FormatNameResolver.CONFIG_READER), FileSourceFactory.EXTERNAL_ONLY_KEYS);
    }

    /**
     * Drives the production chokepoint, {@link FileSourceFactory#validateConfig}: a stored
     * {@code schema_sample_size} reaching a reader that does not claim it must be ignored with a
     * warning, not fail as "unknown option" — identically with and without the {@code _datasource}
     * envelope, whose presence cannot discriminate dataset from inline queries (see
     * {@code FileSourceFactory#LEGACY_VOCABULARY_KEYS}).
     */
    public void testLegacyVocabularyKeyIsIgnoredWithWarningWhenReaderDoesNotClaimIt() {
        FileSourceFactory factory = newStubParquetFactory();
        String location = "s3://bucket/data.parquet";

        // Dataset-originated shape: parent settings ride in under _datasource.
        factory.validateConfig(location, Map.of("schema_sample_size", "50", ExternalSourceResolver.DATASOURCE_CONFIG_KEY, Map.of()));
        assertWarnings(FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "stub-parquet") + "; ignored");

        // Inline shape (no envelope, e.g. an empty-settings data source or a WITH clause): same tolerance.
        factory.validateConfig(location, Map.of("schema_sample_size", "50"));
        assertWarnings(FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "stub-parquet") + "; ignored");

        // The tolerance is scoped to LEGACY_VOCABULARY_KEYS: a genuinely unknown key still fails.
        expectThrows(IllegalArgumentException.class, () -> factory.validateConfig(location, Map.of("not_a_setting", "x")));
    }

    /**
     * Pins {@link FileSourceFactory#LEGACY_VOCABULARY_KEYS} against the format plugins' own
     * {@link FormatSpec}s, not a hand-typed copy: every text format must still claim each legacy key,
     * and the set must be exactly the sampling bound — a closed legacy shim, not a growing vocabulary.
     * Parquet's spec (which must NOT claim it) is not on this classpath; the resolver-aware validator
     * tests and {@code DatasetSchemaSampleSizeValidationIT} cover that side.
     */
    public void testLegacyVocabularyKeysAreClaimedByEveryTextFormat() {
        List<FormatSpec> textSpecs = new ArrayList<>();
        textSpecs.addAll(new CsvDataSourcePlugin().formatSpecs());
        textSpecs.addAll(new NdJsonDataSourcePlugin().formatSpecs());
        assertFalse(textSpecs.isEmpty());
        for (FormatSpec spec : textSpecs) {
            assertTrue(
                "text format [" + spec.format() + "] no longer claims " + FileSourceFactory.LEGACY_VOCABULARY_KEYS,
                spec.configKeys().containsAll(FileSourceFactory.LEGACY_VOCABULARY_KEYS)
            );
        }
        assertEquals(Set.of(FileDataSourceValidator.SCHEMA_SAMPLE_SIZE), FileSourceFactory.LEGACY_VOCABULARY_KEYS);
    }

    /**
     * Real {@link FileSourceFactory} wiring with a reader registered for {@code .parquet} that claims no
     * config keys, standing in for the real Parquet reader (not on this classpath). Under test is the
     * factory's claimed-set composition and warning, not the reader.
     */
    private static FileSourceFactory newStubParquetFactory() {
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("stub-parquet", (s, bf) -> new StubNoConfigReader(), Settings.EMPTY, null);
        formatRegistry.registerExtension(".parquet", "stub-parquet");
        StorageProviderRegistry storageRegistry = new StorageProviderRegistry(Settings.EMPTY);
        storageRegistry.registerFactory("s3", StorageProviderFactory.noConfigKeys(StubStorageProvider::new));
        return new FileSourceFactory(storageRegistry, formatRegistry, new DecompressionCodecRegistry(), Settings.EMPTY);
    }

    /** Claims no config keys (like Parquet does for {@code schema_sample_size}); never actually reads. */
    private static final class StubNoConfigReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException("validateConfig never reads");
        }

        @Override
        public org.elasticsearch.compute.operator.CloseableIterator<org.elasticsearch.compute.data.Page> read(
            StorageObject object,
            FormatReadContext context
        ) {
            throw new UnsupportedOperationException("validateConfig never reads");
        }

        @Override
        public String formatName() {
            return "stub-parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /** Constructible and closeable (validateConfig creates and releases the provider) but never actually reads. */
    private static final class StubStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException("validateConfig never opens storage objects");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException("validateConfig never opens storage objects");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException("validateConfig never opens storage objects");
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException("validateConfig never lists storage objects");
        }

        @Override
        public boolean exists(StoragePath path) {
            throw new UnsupportedOperationException("validateConfig never probes storage objects");
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    public void testFormatIsAFirstClassDatasetKey() {
        assertTrue(
            "format must be part of the dataset vocabulary now that it is a first-class setting",
            FileDataSourceValidator.COORDINATOR_DATASET_KEYS.contains(FormatNameResolver.CONFIG_FORMAT)
        );
    }

    /**
     * The EXTERNAL-only allowlist must itself be a subset of the coordinator keys; otherwise it would
     * be excluding keys the query path does not actually recognise, masking a typo.
     */
    public void testExternalOnlyKeysAreCoordinatorKeys() {
        Set<String> missing = new TreeSet<>(FileSourceFactory.EXTERNAL_ONLY_KEYS);
        missing.removeAll(FileSourceFactory.COORDINATOR_KEYS);
        assertTrue("EXTERNAL_ONLY_KEYS not in COORDINATOR_KEYS: " + missing, missing.isEmpty());
    }

    /**
     * Asserts {@code declared} equals the set of values of every {@code static final String CONFIG_*}
     * constant declared on {@code clazz}. Reflection-based so new constants are picked up without
     * further test changes.
     */
    @SuppressForbidden(reason = "test-only reflection over CONFIG_* constants to pin set/constant symmetry")
    private static void assertConfigKeysMatchConstants(Class<?> clazz, Set<String> declared) {
        Set<String> fromConstants = new TreeSet<>();
        for (Field f : clazz.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) == false || Modifier.isFinal(mods) == false) {
                continue;
            }
            if (f.getType() != String.class) {
                continue;
            }
            if (f.getName().startsWith("CONFIG_") == false) {
                continue;
            }
            f.setAccessible(true);
            try {
                String value = (String) f.get(null);
                if (value != null) {
                    fromConstants.add(value);
                }
            } catch (IllegalAccessException e) {
                throw new AssertionError("cannot read constant " + f.getName(), e);
            }
        }
        Set<String> missingFromKeys = new TreeSet<>(fromConstants);
        missingFromKeys.removeAll(declared);
        Set<String> extraInKeys = new TreeSet<>(declared);
        extraInKeys.removeAll(fromConstants);
        assertTrue(clazz.getSimpleName() + " CONFIG_* constants missing from CONFIG_KEYS: " + missingFromKeys, missingFromKeys.isEmpty());
        assertTrue(clazz.getSimpleName() + " CONFIG_KEYS entries with no backing CONFIG_* constant: " + extraInKeys, extraInKeys.isEmpty());
    }
}
