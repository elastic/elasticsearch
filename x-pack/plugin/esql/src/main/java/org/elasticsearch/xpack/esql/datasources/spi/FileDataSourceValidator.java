/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.FileSplitProvider;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidationUtils.rejectUnknownFields;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidationUtils.validateEnum;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidationUtils.validateInt;

/**
 * {@link DataSourceValidator} for file-based external sources (S3, GCS, Azure).
 *
 * <p>Plugins pass their {@code supportedSchemes()} (e.g. {@code Set.of("s3", "s3a", "s3n")})
 * directly to the constructor — the validator appends {@code "://"} internally when
 * matching resource URIs, so plugins do not need to duplicate the scheme list with
 * URI suffixes.
 *
 * <p>The {@code format} dataset setting selects a file format explicitly (any
 * registered format name, or {@link FormatNameResolver#FORMAT_AUTO} / absent to infer
 * from the resource extension). Format-specific dataset fields (e.g. CSV's
 * {@code delimiter}, Parquet's {@code optimized_reader}) are validated against the
 * <em>resolved</em> format: explicit {@code format} → resource extension → unknown.
 * Both require a {@link FormatConfigKeyResolver} set via
 * {@link #withFormatConfigKeyResolver}. Without a resolver the validator cannot know
 * any format names, so {@code format} and all format-specific fields are rejected and
 * only the base dataset fields are accepted, preserving backward compatibility.
 */
public class FileDataSourceValidator implements DataSourceValidator {

    /**
     * Error shown when a data source is provisioned with federated authentication settings while the
     * {@code esql.datasource.federated_identity.enabled} cluster setting is disabled. The federated fields
     * themselves remain registered on each configuration regardless of the setting, so a PUT carrying them
     * produces this explicit message rather than an "unknown setting" error.
     */
    public static final String FEDERATED_IDENTITY_DISABLED_MESSAGE =
        "federated authentication settings require the [esql.datasource.federated_identity.enabled] cluster setting to be enabled; "
            + "it is disabled by default";

    // Dataset settings are plain values — no secrets. Credentials are inherited from the parent datasource.
    private static final String SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    private static final int SCHEMA_SAMPLE_SIZE_MAX = 1000;

    /**
     * Coordinator-level data-shape keys accepted on a dataset, sourced from each owning component's
     * own {@code CONFIG_KEYS} plus the {@code format} selector. This is exactly
     * {@code FileSourceFactory.COORDINATOR_KEYS} minus the EXTERNAL-only knob ({@code reader}) and the
     * internal {@code _datasource} envelope, a relationship pinned by
     * {@code FileSourceFactoryValidationTests}. Keeping it sourced from the components' constants holds
     * the dataset vocabulary in lockstep with the query path, so a new coordinator key cannot silently
     * regress to EXTERNAL-only.
     */
    public static final Set<String> COORDINATOR_DATASET_KEYS;
    static {
        Set<String> fields = new HashSet<>();
        fields.add(ExternalSourceResolver.CONFIG_SCHEMA_RESOLUTION);
        fields.add(FormatNameResolver.CONFIG_FORMAT);
        fields.addAll(ErrorPolicy.CONFIG_KEYS);
        fields.addAll(PartitionConfig.CONFIG_KEYS);
        fields.addAll(FileSplitProvider.CONFIG_KEYS);
        COORDINATOR_DATASET_KEYS = Set.copyOf(fields);
    }

    /**
     * Full set of base dataset fields accepted by every file-based source, independent of file format:
     * the {@link #COORDINATOR_DATASET_KEYS} (which includes the {@code format} selector) plus the
     * format-agnostic {@code schema_sample_size} sampling bound (which is consumed by the format
     * readers, not the coordinator). Format-specific fields are unioned on per-resource against the
     * resolved format in {@link #validateDataset}.
     */
    private static final Set<String> DATASET_FIELDS;
    static {
        Set<String> fields = new HashSet<>(COORDINATOR_DATASET_KEYS);
        fields.add(SCHEMA_SAMPLE_SIZE);
        DATASET_FIELDS = Set.copyOf(fields);
    }

    /**
     * Base dataset fields excluding the {@code format} selector, used by the no-resolver path: without a
     * {@link FormatConfigKeyResolver} the validator cannot validate a {@code format} value, so it rejects
     * {@code format} (and every format-specific key) just as it did before {@code format} became a
     * first-class setting.
     */
    private static final Set<String> DATASET_FIELDS_WITHOUT_FORMAT;
    static {
        Set<String> fields = new HashSet<>(DATASET_FIELDS);
        fields.remove(FormatNameResolver.CONFIG_FORMAT);
        DATASET_FIELDS_WITHOUT_FORMAT = Set.copyOf(fields);
    }

    private final String type;
    private final BiFunction<Map<String, Object>, Set<String>, DataSourceConfiguration> configFactory;
    private final Set<String> supportedSchemes;
    @Nullable
    private final FormatConfigKeyResolver formatConfigKeyResolver;
    private final Set<String> compressionExtensions;
    private final BooleanSupplier managedIdentityEnabled;
    private final BooleanSupplier federatedIdentityEnabled;

    public FileDataSourceValidator(
        String type,
        BiFunction<Map<String, Object>, Set<String>, DataSourceConfiguration> configFactory,
        Set<String> supportedSchemes
    ) {
        this(type, configFactory, supportedSchemes, null, Set.of(), () -> false, () -> false);
    }

    private FileDataSourceValidator(
        String type,
        BiFunction<Map<String, Object>, Set<String>, DataSourceConfiguration> configFactory,
        Set<String> supportedSchemes,
        @Nullable FormatConfigKeyResolver formatConfigKeyResolver,
        Set<String> compressionExtensions,
        BooleanSupplier managedIdentityEnabled,
        BooleanSupplier federatedIdentityEnabled
    ) {
        this.type = type;
        this.configFactory = configFactory;
        this.supportedSchemes = supportedSchemes;
        this.formatConfigKeyResolver = formatConfigKeyResolver;
        this.compressionExtensions = compressionExtensions;
        this.managedIdentityEnabled = managedIdentityEnabled;
        this.federatedIdentityEnabled = federatedIdentityEnabled;
    }

    /**
     * Returns a new validator that resolves a dataset's file format (from an explicit {@code format}
     * setting or the resource extension) and validates format-specific fields against it. The resolver
     * maps a format name to its config keys, an extension to its format name, and enumerates the known
     * format names for error messages.
     *
     * <p>The {@code compressionExtensions} set restricts compound-extension fallback
     * (e.g. {@code data.csv.gz}) to only known compression suffixes, mirroring the
     * runtime resolution in {@code FormatReaderRegistry}/{@code DecompressionCodecRegistry}.
     */
    public FileDataSourceValidator withFormatConfigKeyResolver(FormatConfigKeyResolver resolver, Set<String> compressionExtensions) {
        return new FileDataSourceValidator(
            type,
            configFactory,
            supportedSchemes,
            resolver,
            compressionExtensions,
            managedIdentityEnabled,
            federatedIdentityEnabled
        );
    }

    /**
     * Returns a new validator that gates {@code auth=managed_identity} on the supplied boolean supplier.
     * The supplier is called on each validation. Pass a live supplier (e.g. backed by an
     * {@code AtomicBoolean} updated via {@code ClusterSettings.addSettingsUpdateConsumer}) so
     * that operator changes to {@code esql.datasource.managed_identity.enabled} take effect
     * without a node restart.
     */
    public FileDataSourceValidator withManagedIdentityEnabled(BooleanSupplier supplier) {
        return new FileDataSourceValidator(
            type,
            configFactory,
            supportedSchemes,
            formatConfigKeyResolver,
            compressionExtensions,
            supplier,
            federatedIdentityEnabled
        );
    }

    /**
     * Returns a new validator that gates federated workload-identity authentication on the supplied boolean supplier.
     * The supplier is called on each validation. Wire it to {@code ExternalSourceSettings#FEDERATED_IDENTITY_ENABLED}
     * in production; tests pass a fixed supplier to exercise both states without depending on a live cluster setting.
     */
    public FileDataSourceValidator withFederatedIdentityEnabled(BooleanSupplier supplier) {
        return new FileDataSourceValidator(
            type,
            configFactory,
            supportedSchemes,
            formatConfigKeyResolver,
            compressionExtensions,
            managedIdentityEnabled,
            supplier
        );
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        return validateDatasource(datasourceSettings, Set.of());
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings, Set<String> existingSecretKeys) {
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            return Map.of();
        }
        DataSourceConfiguration config = configFactory.apply(datasourceSettings, existingSecretKeys);
        if (config instanceof FileDataSourceConfiguration fc && fc.isManagedIdentity() && managedIdentityEnabled.getAsBoolean() == false) {
            throw new ValidationException().addValidationError(FileDataSourceConfiguration.MANAGED_IDENTITY_DISABLED_MESSAGE);
        }
        if (isFederatedIdentityUsed(config) && federatedIdentityEnabled.getAsBoolean() == false) {
            throw new ValidationException().addValidationError(FEDERATED_IDENTITY_DISABLED_MESSAGE);
        }
        return config != null ? config.toStoredSettings() : Map.of();
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();

        validateResource(resource, errors);

        if (datasetSettings == null) {
            datasetSettings = Map.of();
        }

        Map<String, Object> settings = datasetSettings;

        Set<String> acceptedFields = resolveAcceptedFields(resource, settings, errors);
        if (acceptedFields == null) {
            // Bad explicit format: a single "unknown format" error is already recorded. Skip field
            // rejection, per-key parsing and storage so the PUT fails on that one clear reason.
            errors.throwIfValidationErrorsExist();
            return Map.of();
        }

        Map<String, Object> result = new HashMap<>();

        // schema_sample_size keeps its dedicated bounded-int validation, which also stores the parsed int.
        validateInt(settings, result, SCHEMA_SAMPLE_SIZE, 1, SCHEMA_SAMPLE_SIZE_MAX, errors);

        // Strictly validate the data-shape coordinator keys by delegating to the very parsers the
        // query path uses, so a malformed setting is rejected at PUT time with the same message it
        // would produce at query time. Each parser reads the keys it owns from the settings map.
        // error_mode + max_errors + max_error_ratio (incl. mutual exclusion) via the owning policy parser.
        validate(() -> ErrorPolicy.fromConfig(settings, ErrorPolicy.STRICT), errors);
        // partition_detection enum via its owning parser (partition_path/hive_partitioning are free-form,
        // matching the query path which treats any non-"false" hive value as enabled).
        validateEnum(
            settings,
            result,
            PartitionConfig.CONFIG_PARTITIONING_DETECTION,
            PartitionConfig.Strategy.values(),
            PartitionConfig.Strategy::parse,
            errors
        );
        Object schemaResolution = settings.get(ExternalSourceResolver.CONFIG_SCHEMA_RESOLUTION);
        if (schemaResolution != null) {
            validate(() -> FormatReader.SchemaResolution.parse(schemaResolution.toString()), errors);
        }
        Object targetSplitSize = settings.get(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE);
        if (targetSplitSize != null) {
            String trimmedSplitSize = targetSplitSize.toString().trim();
            if (trimmedSplitSize.isEmpty() == false) {
                validate(() -> FileSplitProvider.validateTargetSplitSize(trimmedSplitSize), errors);
            }
        }

        // Normalize the format selector before raw storage so the representation in cluster state
        // is canonical (lowercase, trimmed) regardless of how the user typed it. This ensures that
        // format="CSV" and format="csv" produce identical cluster-state entries and the same
        // SchemaCacheKey string at query time.
        Object rawFormat = settings.get(FormatNameResolver.CONFIG_FORMAT);
        if (rawFormat != null && acceptedFields.contains(FormatNameResolver.CONFIG_FORMAT)) {
            String normalized = FormatNameResolver.normalizeFormatValue(rawFormat);
            if (normalized.isEmpty() == false) {
                result.put(FormatNameResolver.CONFIG_FORMAT, normalized);
            }
        }

        // Store every accepted setting that is present, as its raw value. Each query-time consumer
        // re-parses from value.toString(), so raw storage avoids type-coercion mismatches. Format-specific
        // fields pass through here too; the format reader validates their types at query time. The parsed
        // schema_sample_size and format selector placed above are left intact.
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            if (acceptedFields.contains(entry.getKey()) && result.containsKey(entry.getKey()) == false) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        errors.throwIfValidationErrorsExist();
        return result;
    }

    private boolean isFederatedIdentityUsed(DataSourceConfiguration config) {
        return (config instanceof FileDataSourceConfiguration fc && fc.isFederatedIdentity()) || config.hasFederatedAuth();
    }

    /**
     * Runs an owning component's parser purely for its validation side effect, translating any
     * parse failure into an accumulated CRUD validation error. {@link IllegalArgumentException}
     * covers the enum/number/conflict parsers; {@link ElasticsearchException} covers
     * {@code ByteSizeValue} parse failures from {@link FileSplitProvider#validateTargetSplitSize}.
     */
    private static void validate(Runnable parser, ValidationException errors) {
        try {
            parser.run();
        } catch (IllegalArgumentException | ElasticsearchException e) {
            errors.addValidationError(e.getMessage());
        }
    }

    /**
     * Resolves the set of accepted dataset fields from the resolved file format and records any
     * field-level validation errors against {@code errors} (unknown fields, an unknown explicit
     * {@code format}, or format-specific settings on a resource whose format cannot be determined).
     *
     * <p>The format is resolved as: explicit {@code format} setting → resource extension → unknown.
     * A known format accepts the base fields plus that format's keys (strict). An unknown format
     * accepts the base fields only; a remaining key that some registered format recognises draws a
     * targeted "set format" error, while a key no format recognises is reported as a plain unknown
     * setting. Without a resolver, {@code format} itself is rejected (see {@link #DATASET_FIELDS_WITHOUT_FORMAT}).
     *
     * <p>Returns {@code null} to signal that an explicit {@code format} value is not a registered
     * format: the caller short-circuits on the single {@code unknown format} error rather than piling
     * on per-key messages.
     */
    @Nullable
    private Set<String> resolveAcceptedFields(@Nullable String resource, Map<String, Object> settings, ValidationException errors) {
        if (formatConfigKeyResolver == null) {
            // No registry to validate formats against: reject `format` and every format-specific key.
            rejectUnknownFields(settings, DATASET_FIELDS_WITHOUT_FORMAT, errors);
            return DATASET_FIELDS_WITHOUT_FORMAT;
        }

        String explicitFormat = explicitFormat(settings);
        if (explicitFormat != null) {
            Set<String> formatKeys = formatConfigKeyResolver.configKeysForFormat(explicitFormat);
            if (formatKeys == null) {
                errors.addValidationError(unknownFormatError(explicitFormat, formatConfigKeyResolver.knownFormats()));
                return null;
            }
            return acceptStrict(settings, formatKeys, errors);
        }

        // No usable explicit format: infer the format from the resource extension.
        String extensionFormat = resource == null ? null : formatFromExtension(resource);
        if (extensionFormat != null) {
            Set<String> formatKeys = formatConfigKeyResolver.configKeysForFormat(extensionFormat);
            return acceptStrict(settings, formatKeys != null ? formatKeys : Set.of(), errors);
        }

        // Format unknown: accept the base fields only. Split the remaining keys so each gets the right
        // diagnosis. A key some registered format recognises cannot be validated here (we do not know
        // which format it belongs to), so it draws a targeted "set format" hint — but only when there
        // is a resource URI to anchor the hint to (null resource already produces its own "[resource]
        // is required" error, so format-specific keys fall through to the generic unknown-setting path).
        // A key no format recognises is a genuine typo, reported as a plain unknown setting.
        Set<String> allFormatKeys = resource != null ? allFormatConfigKeys() : Set.of();
        Set<String> needFormat = new TreeSet<>();
        Map<String, Object> unknownKeys = new HashMap<>();
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            String key = entry.getKey();
            if (DATASET_FIELDS.contains(key)) {
                continue;
            }
            if (allFormatKeys.contains(key)) {
                needFormat.add(key);
            } else {
                unknownKeys.put(key, entry.getValue());
            }
        }
        rejectUnknownFields(unknownKeys, DATASET_FIELDS, errors);
        if (needFormat.isEmpty() == false) {
            errors.addValidationError(cannotDetermineFormatError(resource, needFormat));
        }
        return DATASET_FIELDS;
    }

    /**
     * Union of every registered format's config keys. Used by the unknown-format branch to tell a
     * genuine typo apart from a real format-specific setting on a resource whose format cannot be
     * determined. Only called when {@link #formatConfigKeyResolver} is non-null.
     */
    private Set<String> allFormatConfigKeys() {
        Set<String> all = new HashSet<>();
        for (String format : formatConfigKeyResolver.knownFormats()) {
            Set<String> keys = formatConfigKeyResolver.configKeysForFormat(format);
            if (keys != null) {
                all.addAll(keys);
            }
        }
        return all;
    }

    /**
     * The validation error reported when an explicit {@code format} value is not a registered format.
     * Exposed so tests assert the exact message against the same literal production emits, rather than
     * a brittle substring. {@code knownFormats} is sorted for a deterministic message.
     */
    public static String unknownFormatError(String format, Set<String> knownFormats) {
        return "unknown format [" + format + "]; known formats: " + new TreeSet<>(knownFormats);
    }

    /**
     * The validation error reported when a format-specific setting is present but the resource's format
     * cannot be determined (no usable {@code format} value and an unknown or absent extension). Exposed so
     * tests assert the exact message against the same literal production emits. {@code formatSpecificKeys}
     * is sorted for a deterministic message.
     */
    public static String cannotDetermineFormatError(String resource, Set<String> formatSpecificKeys) {
        return "cannot determine format for [" + resource + "]; set \"format\" to use settings like " + new TreeSet<>(formatSpecificKeys);
    }

    /** Accepts the base dataset fields unioned with {@code formatKeys}, rejecting anything else. */
    private static Set<String> acceptStrict(Map<String, Object> settings, Set<String> formatKeys, ValidationException errors) {
        Set<String> accepted = new HashSet<>(DATASET_FIELDS);
        accepted.addAll(formatKeys);
        rejectUnknownFields(settings, accepted, errors);
        return accepted;
    }

    /**
     * Returns the explicit, usable {@code format} value from the settings, or {@code null} when
     * {@code format} is absent, blank, or the {@link FormatNameResolver#FORMAT_AUTO} sentinel (all of
     * which mean "infer from the extension"). Delegates to
     * {@link FormatNameResolver#parseExplicitFormat} so normalization and sentinel handling are
     * single-sourced.
     */
    @Nullable
    private static String explicitFormat(Map<String, Object> settings) {
        return FormatNameResolver.parseExplicitFormat(settings.get(FormatNameResolver.CONFIG_FORMAT));
    }

    /**
     * Resolves the logical format name from a resource's file extension, or {@code null} if the
     * extension maps to no registered format. Handles compound extensions (e.g. {@code data.csv.gz})
     * by stripping a known compression suffix and resolving the inner extension, mirroring the
     * runtime resolution in {@code FormatReaderRegistry}.
     */
    @Nullable
    private String formatFromExtension(String resource) {
        String objectName = extractObjectName(resource);
        if (objectName == null) {
            return null;
        }

        int lastDot = objectName.lastIndexOf('.');
        if (lastDot < 0 || lastDot == objectName.length() - 1) {
            return null;
        }
        String ext = objectName.substring(lastDot).toLowerCase(Locale.ROOT);
        String format = formatConfigKeyResolver.formatForExtension(ext);
        if (format != null) {
            return format;
        }

        // Compound extension: only fall back to the inner extension when the outermost
        // is a known compression suffix (e.g. .gz, .zst). This mirrors the read-path
        // behavior in DecompressionCodecRegistry/FormatReaderRegistry.
        if (compressionExtensions.contains(ext)) {
            String inner = objectName.substring(0, lastDot);
            int innerDot = inner.lastIndexOf('.');
            if (innerDot >= 0 && innerDot < inner.length() - 1) {
                String innerExt = inner.substring(innerDot).toLowerCase(Locale.ROOT);
                return formatConfigKeyResolver.formatForExtension(innerExt);
            }
        }
        return null;
    }

    /** Extracts the object/path portion after the {@code scheme://host/} prefix, stripping any query or fragment. */
    @Nullable
    private static String extractObjectName(String resource) {
        int schemeEnd = resource.indexOf("://");
        if (schemeEnd < 0) {
            return null;
        }
        String afterScheme = resource.substring(schemeEnd + 3);
        int firstSlash = afterScheme.indexOf('/');
        String path;
        if (firstSlash < 0) {
            path = afterScheme;
        } else {
            path = afterScheme.substring(firstSlash + 1);
        }
        int qMark = path.indexOf('?');
        if (qMark >= 0) {
            path = path.substring(0, qMark);
        }
        int hash = path.indexOf('#');
        if (hash >= 0) {
            path = path.substring(0, hash);
        }
        return path;
    }

    private void validateResource(String resource, ValidationException errors) {
        if (resource == null || resource.isBlank()) {
            errors.addValidationError("[resource] is required");
            return;
        }
        // Case-insensitive scheme match. Each plugin declares scheme names without "://" via supportedSchemes();
        // we append "://" here to ensure prefix matching is unambiguous (so e.g. "s3foo://" doesn't match "s3").
        boolean schemeMatch = false;
        for (String scheme : supportedSchemes) {
            String prefix = scheme + "://";
            if (resource.regionMatches(true, 0, prefix, 0, prefix.length())) {
                schemeMatch = true;
                break;
            }
        }
        if (schemeMatch == false) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (String s : supportedSchemes) {
                if (first == false) {
                    sb.append(", ");
                }
                sb.append(s).append("://");
                first = false;
            }
            sb.append(']');
            errors.addValidationError("[resource] must use one of the supported URI schemes " + sb + " but was [" + resource + "]");
        }
    }

    /**
     * Resolves file-format metadata for dataset CRUD validation. Built from all registered
     * {@link FormatSpec} declarations at startup (see {@code EsqlPlugin}).
     */
    public interface FormatConfigKeyResolver {

        /**
         * Builds a resolver from a format-name → config-keys map and an extension → format-name map.
         * Captures immutable copies so the result is safe for concurrent reads, and lowercases the
         * lookup arguments so callers need not normalize first. {@link #knownFormats()} is the
         * config-keys map's key set, so it stays consistent with {@link #configKeysForFormat} by
         * construction. This is the implementation used in production (see {@code EsqlPlugin}); tests
         * use it too rather than hand-rolling a stand-in.
         */
        static FormatConfigKeyResolver of(Map<String, Set<String>> formatConfigKeys, Map<String, String> formatByExtension) {
            Map<String, Set<String>> keysByFormat = Map.copyOf(formatConfigKeys);
            Map<String, String> formatByExt = Map.copyOf(formatByExtension);
            Set<String> formats = keysByFormat.keySet();
            return new FormatConfigKeyResolver() {
                @Override
                public Set<String> configKeysForFormat(String formatName) {
                    return keysByFormat.get(formatName.toLowerCase(Locale.ROOT));
                }

                @Override
                public String formatForExtension(String extension) {
                    return formatByExt.get(extension.toLowerCase(Locale.ROOT));
                }

                @Override
                public Set<String> knownFormats() {
                    return formats;
                }
            };
        }

        /**
         * Returns the set of per-dataset config keys the named format recognises (possibly empty for a
         * known format with no extra keys, e.g. {@code orc}), or {@code null} if the format name is not
         * registered.
         */
        @Nullable
        Set<String> configKeysForFormat(String formatName);

        /**
         * Returns the logical format name (e.g. {@code "ndjson"}) registered for the given file
         * extension (leading dot, lowercased), or {@code null} if the extension maps to no known format.
         */
        @Nullable
        String formatForExtension(String extension);

        /** Returns all registered format names (lowercased); used only for the unknown-format error message. */
        Set<String> knownFormats();
    }
}
