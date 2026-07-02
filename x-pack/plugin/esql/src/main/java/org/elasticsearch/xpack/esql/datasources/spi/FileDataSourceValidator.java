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
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

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
 * <p>Format-specific dataset fields (e.g. CSV's {@code delimiter}, Parquet's
 * {@code optimized_reader}) are accepted when a {@link FormatConfigKeyResolver} is
 * set via {@link #withFormatConfigKeyResolver}. Without a resolver, only the
 * base dataset fields are accepted — preserving backward compatibility.
 */
public class FileDataSourceValidator implements DataSourceValidator {

    // Dataset settings are plain values — no secrets. Credentials are inherited from the parent datasource.
    private static final String SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    private static final int SCHEMA_SAMPLE_SIZE_MAX = 1000;

    /**
     * Coordinator-level data-shape keys accepted on a dataset, sourced from each owning component's
     * own {@code CONFIG_KEYS}. This is exactly {@code FileSourceFactory.COORDINATOR_KEYS} minus the
     * EXTERNAL-only knobs ({@code format}/{@code reader}) and the internal {@code _datasource}
     * envelope — a relationship pinned by {@code FileSourceFactoryValidationTests}. Keeping it sourced
     * from the components' constants holds the dataset vocabulary in lockstep with the query path, so
     * a new coordinator key cannot silently regress to EXTERNAL-only.
     */
    public static final Set<String> COORDINATOR_DATASET_KEYS;
    static {
        Set<String> fields = new HashSet<>();
        fields.add(ExternalSourceResolver.CONFIG_SCHEMA_RESOLUTION);
        fields.addAll(ErrorPolicy.CONFIG_KEYS);
        fields.addAll(PartitionConfig.CONFIG_KEYS);
        fields.addAll(FileSplitProvider.CONFIG_KEYS);
        COORDINATOR_DATASET_KEYS = Set.copyOf(fields);
    }

    /**
     * Full set of base dataset fields accepted by every file-based source, independent of file format:
     * the {@link #COORDINATOR_DATASET_KEYS} plus the format-agnostic {@code schema_sample_size} sampling
     * bound (which is consumed by the format readers, not the coordinator). Format-specific fields are
     * unioned on per-resource via {@link #resolveAcceptedFields(String)}.
     */
    private static final Set<String> DATASET_FIELDS;
    static {
        Set<String> fields = new HashSet<>(COORDINATOR_DATASET_KEYS);
        fields.add(SCHEMA_SAMPLE_SIZE);
        DATASET_FIELDS = Set.copyOf(fields);
    }

    private final String type;
    private final Function<Map<String, Object>, DataSourceConfiguration> configFactory;
    private final Set<String> supportedSchemes;
    @Nullable
    private final FormatConfigKeyResolver formatConfigKeyResolver;
    private final Set<String> compressionExtensions;
    private final BooleanSupplier managedIdentityEnabled;

    public FileDataSourceValidator(
        String type,
        Function<Map<String, Object>, DataSourceConfiguration> configFactory,
        Set<String> supportedSchemes
    ) {
        this(type, configFactory, supportedSchemes, null, Set.of(), () -> false);
    }

    private FileDataSourceValidator(
        String type,
        Function<Map<String, Object>, DataSourceConfiguration> configFactory,
        Set<String> supportedSchemes,
        @Nullable FormatConfigKeyResolver formatConfigKeyResolver,
        Set<String> compressionExtensions,
        BooleanSupplier managedIdentityEnabled
    ) {
        this.type = type;
        this.configFactory = configFactory;
        this.supportedSchemes = supportedSchemes;
        this.formatConfigKeyResolver = formatConfigKeyResolver;
        this.compressionExtensions = compressionExtensions;
        this.managedIdentityEnabled = managedIdentityEnabled;
    }

    /**
     * Returns a new validator that also accepts format-specific dataset fields
     * resolved from the resource's file extension. The resolver maps an extension
     * (e.g. {@code ".csv"}) to the set of config keys the format reader recognises.
     *
     * <p>The {@code compressionExtensions} set restricts compound-extension fallback
     * (e.g. {@code data.csv.gz}) to only known compression suffixes, mirroring the
     * runtime resolution in {@code FormatReaderRegistry}/{@code DecompressionCodecRegistry}.
     */
    public FileDataSourceValidator withFormatConfigKeyResolver(FormatConfigKeyResolver resolver, Set<String> compressionExtensions) {
        return new FileDataSourceValidator(type, configFactory, supportedSchemes, resolver, compressionExtensions, managedIdentityEnabled);
    }

    /**
     * Returns a new validator that gates {@code auth=managed_identity} on the supplied boolean supplier.
     * The supplier is called on each validation. Pass a live supplier (e.g. backed by an
     * {@code AtomicBoolean} updated via {@code ClusterSettings.addSettingsUpdateConsumer}) so
     * that operator changes to {@code esql.datasource.managed_identity.enabled} take effect
     * without a node restart.
     */
    public FileDataSourceValidator withManagedIdentityEnabled(BooleanSupplier supplier) {
        return new FileDataSourceValidator(type, configFactory, supportedSchemes, formatConfigKeyResolver, compressionExtensions, supplier);
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            return Map.of();
        }
        DataSourceConfiguration config = configFactory.apply(datasourceSettings);
        if (config instanceof FileDataSourceConfiguration fc && fc.isManagedIdentity() && managedIdentityEnabled.getAsBoolean() == false) {
            throw new ValidationException().addValidationError(FileDataSourceConfiguration.MANAGED_IDENTITY_DISABLED_MESSAGE);
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

        Set<String> acceptedFields = resolveAcceptedFields(resource);
        rejectUnknownFields(datasetSettings, acceptedFields, errors);

        Map<String, Object> result = new HashMap<>();

        // schema_sample_size keeps its dedicated bounded-int validation, which also stores the parsed int.
        validateInt(datasetSettings, result, SCHEMA_SAMPLE_SIZE, 1, SCHEMA_SAMPLE_SIZE_MAX, errors);

        // Strictly validate the data-shape coordinator keys by delegating to the very parsers the
        // query path uses, so a malformed setting is rejected at PUT time with the same message it
        // would produce at query time. Each parser reads the keys it owns from the settings map.
        Map<String, Object> settings = datasetSettings;
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

        // Store every accepted setting that is present, as its raw value. Each query-time consumer
        // re-parses from value.toString(), so raw storage avoids type-coercion mismatches. Format-specific
        // fields pass through here too; the format reader validates their types at query time. The parsed
        // schema_sample_size already placed above is left intact.
        for (Map.Entry<String, Object> entry : datasetSettings.entrySet()) {
            if (acceptedFields.contains(entry.getKey()) && result.containsKey(entry.getKey()) == false) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        errors.throwIfValidationErrorsExist();
        return result;
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
     * Resolves the full set of accepted dataset fields by unioning the base fields
     * with any format-specific config keys derived from the resource's file extension.
     */
    private Set<String> resolveAcceptedFields(@Nullable String resource) {
        if (formatConfigKeyResolver == null || resource == null) {
            return DATASET_FIELDS;
        }
        Set<String> formatKeys = resolveFormatKeys(resource);
        if (formatKeys.isEmpty()) {
            return DATASET_FIELDS;
        }
        Set<String> union = new HashSet<>(DATASET_FIELDS);
        union.addAll(formatKeys);
        return union;
    }

    /**
     * Extracts the file extension from a resource URI and resolves format-specific
     * config keys. Handles compound extensions (e.g. {@code data.csv.gz}) by
     * stripping a known compression suffix and resolving the inner extension —
     * mirroring the runtime resolution in {@code FormatReaderRegistry}.
     */
    private Set<String> resolveFormatKeys(String resource) {
        String objectName = extractObjectName(resource);
        if (objectName == null) {
            return Set.of();
        }

        int lastDot = objectName.lastIndexOf('.');
        if (lastDot < 0 || lastDot == objectName.length() - 1) {
            return Set.of();
        }
        String ext = objectName.substring(lastDot).toLowerCase(Locale.ROOT);
        Set<String> keys = formatConfigKeyResolver.configKeysForExtension(ext);
        if (keys != null && keys.isEmpty() == false) {
            return keys;
        }

        // Compound extension: only fall back to the inner extension when the outermost
        // is a known compression suffix (e.g. .gz, .zst). This mirrors the read-path
        // behavior in DecompressionCodecRegistry/FormatReaderRegistry.
        if (compressionExtensions.contains(ext)) {
            String inner = objectName.substring(0, lastDot);
            int innerDot = inner.lastIndexOf('.');
            if (innerDot >= 0 && innerDot < inner.length() - 1) {
                String innerExt = inner.substring(innerDot).toLowerCase(Locale.ROOT);
                keys = formatConfigKeyResolver.configKeysForExtension(innerExt);
                if (keys != null) {
                    return keys;
                }
            }
        }
        return Set.of();
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
     * Resolves format-specific configuration keys from a file extension.
     * Built from all registered {@link FormatSpec} declarations at startup.
     */
    @FunctionalInterface
    public interface FormatConfigKeyResolver {
        /**
         * Returns the set of per-dataset config keys the format associated with
         * the given extension recognises, or {@code null} if the extension is
         * unknown or has no registered format.
         */
        @Nullable
        Set<String> configKeysForExtension(String extension);
    }
}
