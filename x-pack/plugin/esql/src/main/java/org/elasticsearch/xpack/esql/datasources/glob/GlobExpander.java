/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.AutoPartitionDetector;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.PartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.TemplatePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expands glob patterns and comma-separated path lists into resolved {@link FileList} instances.
 * Delegates to {@link StorageProvider#listObjects} for directory listing and uses {@link GlobMatcher}
 * for filtering results against the glob pattern.
 * Supports partition-aware glob rewriting when filter hints are provided.
 */
public final class GlobExpander {

    private static final Logger logger = LogManager.getLogger(GlobExpander.class);

    private GlobExpander() {}

    /** Creates a file list from raw entries. Primarily for tests. */
    public static FileList fileListOf(List<StorageEntry> entries, String pattern) {
        return new GenericFileList(entries, pattern);
    }

    /** Creates a file list from raw entries with partition metadata. Primarily for tests. */
    public static FileList fileListOf(List<StorageEntry> entries, String pattern, @Nullable PartitionMetadata partitionMetadata) {
        return new GenericFileList(entries, pattern, partitionMetadata);
    }

    /** Compresses a raw file list into a compact representation (dictionary or Hive-partitioned). */
    public static FileList compact(FileList raw, String basePath) {
        if (raw instanceof GenericFileList generic) {
            return FileListCompactor.compact(basePath, generic);
        }
        return raw;
    }

    /**
     * Expands a glob/comma pattern and compresses the result into a compact representation
     * (DictionaryFileList or DirectoryGroupedFileList). This is the primary entry point for the resolver.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        StoragePath storagePath
    ) throws IOException {
        return expandAndCompact(path, provider, hints, config, storagePath, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Expands a glob/comma pattern and compresses the result, with safety caps on discovery.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        StoragePath storagePath,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        FileList expanded = expand(path, provider, hints, config, maxDiscoveredFiles, maxGlobExpansion);
        if (expanded.isResolved() == false || expanded.fileCount() == 0) {
            return expanded;
        }
        if (expanded instanceof GenericFileList raw) {
            String basePath = storagePath.patternPrefix().toString();
            return FileListCompactor.compact(basePath, raw);
        }
        return expanded;
    }

    /**
     * Expands a whole path — glob or comma-separated list — applying the filter hints. Each glob (a lone pattern, or
     * every segment of a comma list) is expanded through {@link #expandGlobWithRewriteFallback}, which recovers the
     * files a glob rewrite can hide behind a value-spelling mismatch. A comma list is handled per segment so one
     * segment's rewrite-to-empty cannot be masked by another segment that still matches.
     * Objects matching the dataset's {@code file_exclusions} patterns are excluded — by default names beginning
     * with {@code _} or {@code .}, see {@link ExclusionConfig}. Directory placeholder keys (paths ending in
     * {@code /}, and the prefix's own marker) are always skipped.
     */
    public static FileList expand(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        PartitionConfig partitionConfig = PartitionConfig.fromConfig(config);
        ExclusionConfig.NameFilter nameFilter = ExclusionConfig.fromConfig(config).compile();
        return hasTopLevelComma(path)
            ? doExpandCommaSeparated(path, provider, hints, partitionConfig, maxDiscoveredFiles, maxGlobExpansion, nameFilter)
            : expandGlobWithRewriteFallback(path, provider, hints, partitionConfig, maxDiscoveredFiles, maxGlobExpansion, nameFilter);
    }

    /**
     * Expands a single glob, falling back to the un-rewritten glob if — and only if — a partition rewrite narrowed it
     * to nothing. Hint-based narrowing is only an optimisation: the query's filter is still evaluated on the rows, so
     * listing a superset is always correct while listing a subset is a wrong answer.
     *
     * <p>Only the glob rewrite ({@link #effectivePattern}/{@link #rewriteSegment}) can hide files: it spells a value
     * with {@link String#valueOf}, so {@code WHERE month == 6} narrows the glob to {@code month=6} while the Hive
     * convention writes a zero-padded {@code month=06}. Reporting empty there would be silent zero rows on an ordinary
     * dataset, so we re-list the un-rewritten glob — keeping the {@code _file.*} filters, which are exact and cannot
     * hide anything — and let the row filter decide. If the un-rewritten glob is empty too, the pattern genuinely
     * matches nothing and the caller's "matched no files" error stands. When the rewrite did not change the pattern
     * there is nothing to disambiguate, so we expand once with no retry.
     */
    private static FileList expandGlobWithRewriteFallback(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        ExclusionConfig.NameFilter nameFilter
    ) throws IOException {
        if (effectivePattern(pattern, hints, partitionConfig).equals(pattern)) {
            return doExpandGlob(pattern, provider, hints, partitionConfig, maxDiscoveredFiles, maxGlobExpansion, nameFilter);
        }
        // The retry drops the rewrite but keeps the exact _file.* filters, so it can only come back empty when the
        // un-rewritten glob genuinely matches nothing.
        List<PartitionFilterHint> fileHintsOnly = fileMetadataHints(hints);
        FileList expanded;
        try {
            expanded = doExpandGlob(pattern, provider, hints, partitionConfig, maxDiscoveredFiles, maxGlobExpansion, nameFilter);
        } catch (IOException e) {
            // The rewritten prefix may name a folder that does not exist; the local filesystem throws where object
            // stores return empty. Both mean the rewrite, not the dataset, emptied the listing — retry either way.
            logger.debug(() -> "Rewritten listing of [" + pattern + "] failed; re-listing without the glob rewrite", e);
            try {
                return doExpandGlob(pattern, provider, fileHintsOnly, partitionConfig, maxDiscoveredFiles, maxGlobExpansion, nameFilter);
            } catch (IOException retryFailure) {
                retryFailure.addSuppressed(e);
                throw retryFailure;
            }
        }
        if (expanded.isResolved() && expanded.fileCount() == 0) {
            logger.debug("Rewrite of [{}] narrowed to an empty listing; re-listing without the glob rewrite", pattern);
            // A full re-list can exceed max_discovered_files and throw, exactly as the un-filtered query would; that
            // cap error is preserved deliberately — deciding spelling-miss vs genuinely-empty needs the full listing.
            return doExpandGlob(pattern, provider, fileHintsOnly, partitionConfig, maxDiscoveredFiles, maxGlobExpansion, nameFilter);
        }
        return expanded;
    }

    /**
     * The single place that decides which detector a resolved {@link PartitionConfig} selects. Takes a non-null
     * config: the listing boundary always resolves one, so a null here is a programming error rather than a
     * user-reachable state.
     */
    public static PartitionDetector resolveDetector(PartitionConfig config) {
        return switch (config.strategy()) {
            case NONE -> null;
            case HIVE -> HivePartitionDetector.INSTANCE;
            case TEMPLATE -> {
                String template = config.pathTemplate();
                // A template that names no columns cannot build a detector: parseTemplateColumns matches a segment
                // in full, so `year={year}` contributes none, and TemplatePartitionDetector's constructor rejects
                // that. Falling back to Hive is what such a dataset resolved to before the setting reached the read
                // path, so a stored one keeps its columns instead of failing every query.
                if (template == null || TemplatePartitionDetector.parseTemplateColumns(template).isEmpty()) {
                    yield HivePartitionDetector.INSTANCE;
                }
                yield new TemplatePartitionDetector(template);
            }
            case AUTO -> AutoPartitionDetector.fromConfig(config);
        };
    }

    /**
     * Returns true if the given path string represents multiple files — either because it contains
     * glob metacharacters in the path component, or because it is a comma-separated list.
     *
     * IPv6 host literals in URL authorities (e.g. {@code http://[::1]/data/*.parquet}) use bracket
     * notation per RFC 3986 §3.2.2. Those brackets are parsed as part of the authority, not the
     * path, so they are not treated as glob character-class syntax.
     */
    public static boolean isMultiFile(String path) {
        if (path == null) {
            return false;
        }
        if (hasTopLevelComma(path)) {
            return true;
        }
        // Only scan the path component for glob metacharacters, not the full URL string.
        // This prevents IPv6 bracket notation in the authority from being mistaken for
        // a glob character class.
        try {
            return StoragePath.of(path).isPattern();
        } catch (IllegalArgumentException e) {
            // Not a parseable URL; fall back to scanning the whole string
            for (char c : StoragePath.GLOB_METACHARACTERS) {
                if (path.indexOf(c) >= 0) {
                    return true;
                }
            }
            return false;
        }
    }

    public static FileList expandGlob(String pattern, StorageProvider provider) throws IOException {
        return expandGlob(pattern, provider, null, (Map<String, Object>) null);
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config
    ) throws IOException {
        ExclusionConfig.NameFilter nameFilter = ExclusionConfig.fromConfig(config).compile();
        return doExpandGlob(pattern, provider, hints, PartitionConfig.fromConfig(config), Integer.MAX_VALUE, Integer.MAX_VALUE, nameFilter);
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        ExclusionConfig.NameFilter nameFilter = ExclusionConfig.fromConfig(config).compile();
        return doExpandGlob(pattern, provider, hints, PartitionConfig.fromConfig(config), maxDiscoveredFiles, maxGlobExpansion, nameFilter);
    }

    static FileList doExpandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        ExclusionConfig.NameFilter nameFilter
    ) throws IOException {
        Check.notNull(pattern, "pattern cannot be null");
        Check.notNull(provider, "provider cannot be null");

        String effectivePattern = effectivePattern(pattern, hints, partitionConfig);

        StoragePath storagePath = StoragePath.of(effectivePattern);

        if (storagePath.isPattern() == false) {
            if (effectivePattern.equals(pattern)) {
                return FileList.UNRESOLVED;
            }
            // Hints resolved all wildcards to a concrete path — resolve via exists()
            var obj = provider.newObject(storagePath);
            if (obj.exists()) {
                StorageEntry entry = new StorageEntry(storagePath, obj.length(), obj.lastModified());
                PartitionMetadata partitionMetadata = detectPartitions(List.of(entry), partitionConfig);
                return new GenericFileList(List.of(entry), pattern, partitionMetadata);
            }
            return FileList.EMPTY;
        }

        StoragePath prefix = storagePath.patternPrefix();
        String glob = storagePath.globPart();

        // One reader of the pattern. The matcher is built first, and it answers whether the pattern names a finite
        // set of keys — which is a property of the parse, not something to rediscover by scanning the characters
        // again. A second scan was a second opinion that had to agree with the matcher by hand; when the two drifted
        // the only symptom was silently choosing the wrong strategy.
        GlobMatcher matcher = new GlobMatcher(glob);

        // Enumerable pattern: probe each key with exists() instead of listing a prefix that may hold millions.
        List<String> candidates = matcher.enumerateKeys(maxGlobExpansion);
        if (candidates != null) {
            List<StorageEntry> matched = new ArrayList<>();
            String prefixStr = prefix.toString();
            for (String candidate : candidates) {
                StoragePath fullPath = StoragePath.of(prefixStr + candidate);
                var obj = provider.newObject(fullPath);
                if (obj.exists()) {
                    matched.add(new StorageEntry(fullPath, obj.length(), obj.lastModified()));
                }
                checkDiscoveredFilesLimit(matched.size(), maxDiscoveredFiles);
            }
            if (hints != null && hints.isEmpty() == false) {
                matched = applyFileFiltersRetainingAnchor(matched, hints);
            }
            if (matched.isEmpty()) {
                return FileList.EMPTY;
            }
            matched.sort(Comparator.comparing(e -> e.path().toString()));
            PartitionMetadata partitionMetadata = detectPartitions(matched, partitionConfig);
            return new GenericFileList(matched, pattern, partitionMetadata);
        }

        boolean recursive = matcher.needsRecursion();

        List<StorageEntry> matched = new ArrayList<>();
        String prefixStr = prefix.toString();

        try (StorageIterator iterator = provider.listObjects(prefix, recursive)) {
            while (iterator.hasNext()) {
                StorageEntry entry = iterator.next();
                String entryPath = entry.path().toString();
                String relativePath;
                if (entryPath.startsWith(prefixStr)) {
                    relativePath = entryPath.substring(prefixStr.length());
                } else {
                    // Defensive fallback: provider returned a path that does not begin with the listing prefix.
                    // objectName() yields only the last component, so the exclusion check below will miss a hidden
                    // intermediate directory (e.g. _delta_log/file.json → sees "file.json", not "_delta_log").
                    // TODO: investigate which providers hit this branch and whether they can be fixed upstream.
                    relativePath = entry.path().objectName();
                }
                if (relativePath.isEmpty() || relativePath.endsWith("/")) {
                    // Directory placeholder key (e.g. the S3 console "folder" object). These are not files, so they
                    // are skipped as listing normalization rather than left to exclusion policy — a dataset should
                    // not have to configure away an artefact of how a console represents a folder.
                    //
                    // The empty case is the placeholder for the listing prefix ITSELF — listing `s3://b/data/*`
                    // returns the key `s3://b/data/`, whose path relative to the prefix is "". It is not caught by
                    // the endsWith check, and a `*` glob matches the empty string, so without this the marker
                    // reaches the reader and fails the query naming an object the user never referenced.
                    continue;
                }
                if (matcher.matches(relativePath) && nameFilter.keeps(relativePath)) {
                    matched.add(entry);
                    checkDiscoveredFilesLimit(matched.size(), maxDiscoveredFiles);
                }
            }
        }

        // Apply file metadata filters from WHERE clause hints (e.g., _file.modified > X, _file.size > Y).
        // This prunes files at listing time — before any data is read.
        if (hints != null && hints.isEmpty() == false) {
            matched = applyFileFiltersRetainingAnchor(matched, hints);
        }

        if (matched.isEmpty()) {
            return FileList.EMPTY;
        }

        matched.sort(Comparator.comparing(e -> e.path().toString()));

        PartitionMetadata partitionMetadata = detectPartitions(matched, partitionConfig);

        return new GenericFileList(matched, pattern, partitionMetadata);
    }

    /**
     * Applies the {@code _file.*} filters, but keeps the unfiltered listing when they prune a non-empty listing to
     * nothing. Those filters are exact, so an all-pruned result is genuinely zero rows — but the resolver needs at
     * least one file to anchor schema inference, and the split- and row-level filters still yield zero rows from the
     * retained listing. A partial prune (some files survive) stands untouched; only the all-pruned case is retained,
     * which also keeps that emptiness out of {@link #expandGlobWithRewriteFallback}'s rewrite-only retry.
     */
    private static List<StorageEntry> applyFileFiltersRetainingAnchor(List<StorageEntry> matched, List<PartitionFilterHint> hints) {
        List<StorageEntry> filtered = applyFileMetadataFilters(matched, hints);
        return filtered.isEmpty() && matched.isEmpty() == false ? matched : filtered;
    }

    /**
     * The partition columns a listing carries, decided entirely by the resolved {@link PartitionConfig}. One input,
     * one decision: no separate enable flag and no raw settings map alongside it.
     */
    static PartitionMetadata detectPartitions(List<StorageEntry> files, PartitionConfig partitionConfig) {
        if (PartitionConfig.Strategy.NONE == partitionConfig.strategy()) {
            return null;
        }
        PartitionDetector detector = resolveDetector(partitionConfig);
        if (detector == null) {
            return null;
        }
        PartitionMetadata result = detector.detect(files);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return result;
    }

    private static void checkDiscoveredFilesLimit(int discoveredCount, int maxDiscoveredFiles) {
        Check.clientError(
            discoveredCount <= maxDiscoveredFiles,
            "Glob pattern discovered too many files ({}, limit {}). Narrow your glob pattern, add partition "
                + "filters, or increase the [esql.external.max_discovered_files] cluster setting.",
            discoveredCount,
            maxDiscoveredFiles
        );
    }

    public static FileList expandCommaSeparated(String pathList, StorageProvider provider) throws IOException {
        return expandCommaSeparated(pathList, provider, null, (Map<String, Object>) null);
    }

    public static FileList expandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config
    ) throws IOException {
        return doExpandCommaSeparated(
            pathList,
            provider,
            hints,
            PartitionConfig.fromConfig(config),
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            ExclusionConfig.fromConfig(config).compile()
        );
    }

    public static FileList expandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        return doExpandCommaSeparated(
            pathList,
            provider,
            hints,
            PartitionConfig.fromConfig(config),
            maxDiscoveredFiles,
            maxGlobExpansion,
            ExclusionConfig.fromConfig(config).compile()
        );
    }

    private static FileList doExpandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        ExclusionConfig.NameFilter nameFilter
    ) throws IOException {
        Check.notNull(pathList, "pathList cannot be null");
        Check.notNull(provider, "provider cannot be null");

        List<StorageEntry> allEntries = new ArrayList<>();

        for (String trimmed : commaSegments(pathList)) {
            StoragePath segmentPath = StoragePath.of(trimmed);
            if (segmentPath.isPattern()) {
                int remainingBudget = maxDiscoveredFiles - allEntries.size();
                // Per segment, so a segment a rewrite narrows to empty falls back on its own instead of being masked
                // by another segment that still matches.
                FileList expanded = expandGlobWithRewriteFallback(
                    trimmed,
                    provider,
                    hints,
                    partitionConfig,
                    remainingBudget,
                    maxGlobExpansion,
                    nameFilter
                );
                if (expanded instanceof GenericFileList g && expanded.fileCount() > 0) {
                    allEntries.addAll(g.files());
                }
            } else {
                var obj = provider.newObject(segmentPath);
                if (obj.exists()) {
                    allEntries.add(new StorageEntry(segmentPath, obj.length(), obj.lastModified()));
                    checkDiscoveredFilesLimit(allEntries.size(), maxDiscoveredFiles);
                }
            }
        }

        if (allEntries.isEmpty()) {
            return FileList.EMPTY;
        }

        allEntries.sort(Comparator.comparing(e -> e.path().toString()));

        PartitionMetadata partitionMetadata = detectPartitions(allEntries, partitionConfig);

        return new GenericFileList(allEntries, pathList, partitionMetadata);
    }

    /**
     * The pattern an expansion will actually list once the hints have narrowed it. The one place that decides
     * whether a hint reaches the glob at all; {@link #doExpandGlob} and {@link #listingCacheDiscriminator} both
     * route through it so that the listing cache key cannot drift from the listing it names.
     */
    static String effectivePattern(String pattern, @Nullable List<PartitionFilterHint> hints, PartitionConfig partitionConfig) {
        if (hints == null || hints.isEmpty() || PartitionConfig.Strategy.NONE == partitionConfig.strategy()) {
            return pattern;
        }
        return rewriteGlobWithHints(pattern, hints, partitionConfig);
    }

    /**
     * Everything about a query that determines which files a {@code path} lists: the resolved
     * {@link PartitionConfig} (strategy AND path template),
     * the effective (post-rewrite) glob pattern, the {@code _file.*} metadata filters, and the resolved
     * {@link ExclusionConfig}. These are the inputs
     * {@link #doExpandGlob} consults beyond the storage contents themselves — the rewrite via {@link #effectivePattern}
     * and the file filters via {@link #applyFileMetadataFilters} — and this value shares those same helpers, so the
     * listing cache key it feeds cannot drift from the listing it names. Note this binds only the cache key: a new
     * hint channel added to {@link #doExpandGlob} must be added here by hand, or that channel silently reintroduces
     * the poisoning bug.
     *
     * <p>{@link #encode} is injective — every variable-length piece is length-prefixed, so no user-controlled filter
     * literal (which may hold any character, including the delimiters) can forge a field boundary and collide two
     * different filters onto one key. Equal encodings therefore genuinely mean equal listings. A new field added to
     * this record joins {@code equals} for free but must be added to {@code encode} by hand to stay in the key.
     */
    private record ListingIdentity(
        PartitionConfig partitionConfig,
        String effectivePattern,
        List<String> encodedFileHints,
        ExclusionConfig exclusionConfig
    ) {

        static ListingIdentity of(
            String path,
            @Nullable List<PartitionFilterHint> hints,
            PartitionConfig partitionConfig,
            ExclusionConfig exclusionConfig
        ) {
            return new ListingIdentity(
                partitionConfig,
                effectiveWholePathPattern(path, hints, partitionConfig),
                encodedFileMetadataHints(hints),
                exclusionConfig
            );
        }

        String encode() {
            StringBuilder sb = new StringBuilder();
            // Strategy AND template both discriminate: the cached FileList carries its PartitionMetadata, and two
            // datasets on one glob with the same strategy but different templates produce different partition
            // columns from an identical effective pattern. The template is user-controlled free text, so it is
            // length-prefixed like every other variable-length field; the null marker keeps a null template and an
            // empty one distinct so injectivity stays trivially provable.
            appendLengthPrefixed(sb, partitionConfig.strategy().name());
            sb.append(partitionConfig.pathTemplate() == null ? '0' : '1');
            appendLengthPrefixed(sb, partitionConfig.pathTemplate() == null ? "" : partitionConfig.pathTemplate());
            appendLengthPrefixed(sb, effectivePattern);
            sb.append(encodedFileHints.size()).append(':');
            for (String encodedHint : encodedFileHints) {
                appendLengthPrefixed(sb, encodedHint);
            }
            // The exclusion list is framed like encodedFileHints above — a count, then that many
            // length-prefixed entries — so no user-supplied glob can forge a field boundary. Entry order is
            // preserved rather than sorted: order does not change semantics (any-match), so two same-set
            // different-order configs merely get distinct keys, which is safe over-fragmentation and keeps
            // encode() aligned with the record's equals.
            sb.append(exclusionConfig.fileExclusions().size()).append(':');
            for (String glob : exclusionConfig.fileExclusions()) {
                appendLengthPrefixed(sb, glob);
            }
            return sb.toString();
        }
    }

    /** Appends {@code <charLength>':'<value>}, an injective framing that no value content can forge a boundary in. */
    private static void appendLengthPrefixed(StringBuilder sb, String value) {
        sb.append(value.length()).append(':').append(value);
    }

    /**
     * A string that identifies the listing a given set of hints produces for a given path: equal discriminators
     * guarantee equal listings, so it is safe to key the listing cache on it. See {@link ListingIdentity} for the
     * inputs it captures and why they are exhaustive; hints that reach none of them (an ordinary data column, say)
     * leave the discriminator untouched, so an incidentally-filtered query still shares the un-filtered entry.
     * The exclusion settings resolve from {@code config} via {@link ExclusionConfig#fromConfig}.
     */
    public static String listingCacheDiscriminator(
        String path,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config
    ) {
        return ListingIdentity.of(path, hints, PartitionConfig.fromConfig(config), ExclusionConfig.fromConfig(config)).encode();
    }

    /**
     * Mirrors {@link #expand}'s glob/comma dispatch: in a comma list only the pattern segments are rewritten, over
     * the same {@link #commaSegments} decomposition the expansion walks, against the same resolved
     * {@link PartitionConfig} the expansion uses.
     */
    private static String effectiveWholePathPattern(
        String path,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig
    ) {
        if (hasTopLevelComma(path) == false) {
            return effectivePattern(path, hints, partitionConfig);
        }
        List<String> segments = commaSegments(path);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String segment = segments.get(i);
            sb.append(isPattern(segment) ? effectivePattern(segment, hints, partitionConfig) : segment);
        }
        return sb.toString();
    }

    /**
     * The non-empty, trimmed segments of a comma-separated path list. The one decomposition shared by the expansion
     * ({@link #doExpandCommaSeparated}) and the identity that names its result ({@link #effectiveWholePathPattern}),
     * so the two cannot disagree on which segments a path has.
     */
    private static List<String> commaSegments(String pathList) {
        List<String> segments = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < pathList.length(); i++) {
            char c = pathList.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth = Math.max(0, depth - 1);
            } else if (c == ',' && depth == 0) {
                addSegment(segments, pathList.substring(start, i));
                start = i + 1;
            }
        }
        addSegment(segments, pathList.substring(start));
        return segments;
    }

    private static void addSegment(List<String> segments, String segment) {
        String trimmed = segment.trim();
        if (trimmed.isEmpty() == false) {
            segments.add(trimmed);
        }
    }

    /**
     * Whether a path holds a top-level comma, i.e. is a list of resources rather than one. A comma inside a brace
     * group belongs to the glob, not to the list: splitting on it blindly tore {@code s3://b/x.{csv,tsv}} into
     * {@code s3://b/x.{csv} and {@code tsv}}, and the second fragment failed for having no scheme — so brace
     * alternation, which the matcher has always supported, was unusable through the entry point datasets take.
     */
    private static boolean hasTopLevelComma(String path) {
        int depth = 0;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth = Math.max(0, depth - 1);
            } else if (c == ',' && depth == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a comma-list segment is a glob, matching {@link #doExpandCommaSeparated}'s test. An unparseable
     * segment is reported as a non-pattern so that building a cache key never fails: the expansion that follows
     * raises the parse error, exactly as it does today.
     */
    private static boolean isPattern(String segment) {
        try {
            return StoragePath.of(segment).isPattern();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** The hints {@link #applyFileMetadataFilters} acts on, each encoded injectively (length-prefixed) and ordered. */
    private static List<String> encodedFileMetadataHints(@Nullable List<PartitionFilterHint> hints) {
        List<PartitionFilterHint> fileHints = fileMetadataHints(hints);
        if (fileHints.isEmpty()) {
            return List.of();
        }
        List<String> encoded = new ArrayList<>(fileHints.size());
        for (PartitionFilterHint hint : fileHints) {
            StringBuilder sb = new StringBuilder();
            appendLengthPrefixed(sb, hint.columnName());
            appendLengthPrefixed(sb, hint.operator().name());
            sb.append(hint.values().size()).append(':');
            for (Object value : hint.values()) {
                // The value's type is part of its identity: _file.name == "6" and _file.name == 6 filter differently.
                appendLengthPrefixed(sb, value == null ? "\0null" : value.getClass().getName());
                appendLengthPrefixed(sb, value == null ? "\0null" : value.toString());
            }
            encoded.add(sb.toString());
        }
        Collections.sort(encoded);
        return encoded;
    }

    /** The subset of hints that {@link #applyFileMetadataFilters} prunes the listing with. */
    static List<PartitionFilterHint> fileMetadataHints(@Nullable List<PartitionFilterHint> hints) {
        if (hints == null || hints.isEmpty()) {
            return List.of();
        }
        List<PartitionFilterHint> fileHints = new ArrayList<>();
        for (PartitionFilterHint hint : hints) {
            if (FileMetadataColumns.isFileMetadataColumn(hint.columnName())) {
                fileHints.add(hint);
            }
        }
        return fileHints;
    }

    public static String rewriteGlobWithHints(String pattern, List<PartitionFilterHint> hints) {
        return rewriteGlobWithHints(pattern, hints, null);
    }

    public static String rewriteGlobWithHints(String pattern, List<PartitionFilterHint> hints, @Nullable PartitionConfig partitionConfig) {
        Map<String, PartitionFilterHint> rewritableHints = indexRewritableHints(hints);
        if (rewritableHints.isEmpty()) {
            return pattern;
        }

        // Only a TEMPLATE strategy may drive the template rewrite. It narrows the glob to the template's spelling of
        // the value — a bare segment — which is sound only when detection is certainly template-based. Under HIVE a
        // coincidental bare folder (data/2024/) would be listed instead of the real data/year=2024/, and because that
        // listing is non-empty the rewrite-to-empty fallback never fires, so the query returns the wrong rows rather
        // than a superset. Under AUTO, detection may still resolve to Hive at detect time. HIVE and AUTO keep the
        // key=value segment rewrite below, which is what they had before the setting reached the read path.
        if (partitionConfig != null
            && PartitionConfig.Strategy.TEMPLATE == partitionConfig.strategy()
            && partitionConfig.pathTemplate() != null) {
            String templateRewritten = rewriteGlobWithTemplate(pattern, rewritableHints, partitionConfig.pathTemplate());
            if (templateRewritten != null) {
                return templateRewritten;
            }
            // Under TEMPLATE the column value is the WHOLE directory segment, so the key=value rewrite below would
            // narrow on the wrong axis: for a template-bound value of "part=a" it would spell the segment
            // "part=part=a" and list a sibling directory of that literal name instead. That listing is non-empty, so
            // the rewrite-to-empty fallback would not fire. The rewrite is never useful under TEMPLATE anyway — the
            // template rewrite above is the only one that matches how the value was bound.
            return pattern;
        }

        String[] segments = pattern.split("/");
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                result.append('/');
            }
            result.append(rewriteSegment(segments[i], rewritableHints));
        }
        return result.toString();
    }

    public static String rewriteGlobWithTemplate(String pattern, Map<String, PartitionFilterHint> rewritableHints, String template) {
        List<String> templateColumns = TemplatePartitionDetector.parseTemplateColumns(template);
        if (templateColumns.isEmpty()) {
            return null;
        }

        String[] segments = pattern.split("/");
        List<Integer> wildcardPositions = new ArrayList<>();
        for (int i = 0; i < segments.length; i++) {
            if ("*".equals(segments[i])) {
                wildcardPositions.add(i);
            }
        }

        if (wildcardPositions.size() < templateColumns.size()) {
            return null;
        }

        // Map template columns to wildcard positions (positional mapping)
        int offset = wildcardPositions.size() - templateColumns.size();
        boolean changed = false;
        for (int t = 0; t < templateColumns.size(); t++) {
            String colName = templateColumns.get(t);
            PartitionFilterHint hint = rewritableHints.get(colName);
            if (hint == null) {
                continue;
            }
            int segIdx = wildcardPositions.get(offset + t);
            List<Object> values = hint.values();
            if (hint.isSingleValue()) {
                String value = String.valueOf(values.get(0));
                if (globExpressible(value) == false) {
                    continue; // leave the wildcard so the full set is listed (superset); the row filter narrows
                }
                segments[segIdx] = value;
            } else {
                Set<String> spellings = partitionValueSpellings(values);
                if (braceExpressible(spellings) == false) {
                    continue; // leave the wildcard so the full set is listed (superset); the row filter narrows
                }
                segments[segIdx] = brace(spellings);
            }
            changed = true;
        }

        if (changed == false) {
            return null;
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                result.append('/');
            }
            result.append(segments[i]);
        }
        return result.toString();
    }

    private static Map<String, PartitionFilterHint> indexRewritableHints(List<PartitionFilterHint> hints) {
        Map<String, PartitionFilterHint> byColumn = Maps.newHashMapWithExpectedSize(hints.size());
        for (PartitionFilterHint hint : hints) {
            if (hint.operator().canRewriteGlob()) {
                byColumn.putIfAbsent(hint.columnName(), hint);
            }
        }
        return byColumn;
    }

    private static String rewriteSegment(String segment, Map<String, PartitionFilterHint> rewritableHints) {
        int eqIdx = segment.indexOf('=');
        if (eqIdx <= 0 || eqIdx >= segment.length() - 1) {
            return segment;
        }

        String key = segment.substring(0, eqIdx);
        String valuePart = segment.substring(eqIdx + 1);
        if ("*".equals(valuePart) == false) {
            return segment;
        }

        PartitionFilterHint hint = rewritableHints.get(key);
        if (hint == null) {
            return segment;
        }

        List<Object> values = hint.values();
        if (hint.isSingleValue()) {
            String value = String.valueOf(values.get(0));
            return globExpressible(value) ? key + "=" + value : segment;
        }

        // Multiple values: use glob brace syntax key={v1,v2,...}, each value in every on-disk spelling. If a value
        // holds a brace delimiter the glob dialect cannot express, leave the wildcard so the full set is listed.
        Set<String> spellings = partitionValueSpellings(values);
        if (braceExpressible(spellings) == false) {
            return segment;
        }
        return key + "=" + brace(spellings);
    }

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    /**
     * The ASCII characters Hive/Spark percent-escape in a partition folder name ({@code FileUtils.escapePathName} /
     * {@code ExternalCatalogUtils.escapePathName}): the C0 control range, {@code DEL}, and a fixed punctuation set.
     * Space, {@code ,} and {@code }} are deliberately NOT escaped by those writers (so {@link #braceExpressible}
     * still vetoes comma/brace values to a full-glob listing). Non-ASCII passes through literally, matching the writer.
     */
    private static final BitSet HIVE_ESCAPE = new BitSet(128);
    static {
        for (int c = 0; c < 0x20; c++) {
            HIVE_ESCAPE.set(c);
        }
        HIVE_ESCAPE.set(0x7F);
        for (char c : new char[] { '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '{', '[', ']', '^' }) {
            HIVE_ESCAPE.set(c);
        }
    }

    /**
     * The on-disk spellings an {@code IN}-list of partition value can take, so the glob rewrite (which matches folder
     * names literally) lists every folder the row filter would keep instead of silently dropping some. For each value:
     * the value itself; its two-digit zero-padded form when a single digit (Hive convention for
     * {@code month}/{@code day}/{@code hour}, {@code 6 → 06}); and the Hive/Spark percent-escaped form of each
     * ({@code ns:click → ns%3Aclick}, the everyday shape for string partitions holding {@code :} or {@code /}, e.g.
     * timestamps). Every spelling only <b>widens</b> the brace, so the listing is always a superset and the row filter
     * narrows — a wrong spelling can never drop rows, only add a folder that is then filtered out.
     *
     * <p>A single {@code EQUALS} value stays a concrete segment (it prefix-narrows the listing; a spelling miss lists
     * empty and hits the un-rewritten fallback). Remaining gaps — integer-vs-decimal spelling ({@code 6} vs
     * {@code 6.0}, a typing mismatch better fixed by matching folders by typed value), boolean case, padding wider
     * than two digits, and mixed-width padding within one column — either hit the empty-listing fallback or are the
     * value-aware follow-up; they cannot be solved cleanly by widening spellings (e.g. blindly emitting {@code 6.0}
     * for every integer would list nonsense {@code year=2024.0}).
     */
    private static Set<String> partitionValueSpellings(List<Object> values) {
        Set<String> spellings = new LinkedHashSet<>();
        for (Object value : values) {
            for (String variant : valueVariants(String.valueOf(value))) {
                spellings.add(variant);
                String escaped = hiveEscape(variant);
                if (escaped.equals(variant) == false) {
                    spellings.add(escaped);
                }
            }
        }
        return spellings;
    }

    /** A value's unescaped on-disk forms before writer escaping: itself and its 2-zero-padded single-digit form. */
    private static List<String> valueVariants(String raw) {
        if (raw.length() == 1 && raw.charAt(0) >= '0' && raw.charAt(0) <= '9') {
            return List.of(raw, "0" + raw);
        }
        return List.of(raw);
    }

    /** Percent-escapes {@code value} the way Hive/Spark write partition folder names (see {@link #HIVE_ESCAPE}). */
    private static String hiveEscape(String value) {
        StringBuilder sb = null;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 128 && HIVE_ESCAPE.get(c)) {
                if (sb == null) {
                    sb = new StringBuilder(value.length() + 6).append(value, 0, i);
                }
                sb.append('%').append(HEX[(c >> 4) & 0xF]).append(HEX[c & 0xF]);
            } else if (sb != null) {
                sb.append(c);
            }
        }
        return sb == null ? value : sb.toString();
    }

    /**
     * Whether a set of spellings can be spliced into a glob as brace alternatives. A value containing {@code ,} or
     * {@code }} would be mis-split by the brace parser (into separate alternatives, or a truncated brace), turning
     * one value into several and dropping the folder that literally contains the delimiter — so when any spelling
     * holds one, the caller must skip the rewrite and list the full glob (a superset) rather than a wrong subset.
     */
    private static boolean braceExpressible(Set<String> spellings) {
        for (String spelling : spellings) {
            if (spelling.indexOf(',') >= 0 || spelling.indexOf('}') >= 0 || globExpressible(spelling) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a partition value can be spliced into a glob as itself. A value holding a glob metacharacter cannot:
     * spliced raw it would be read as a wildcard and widen the listing, which is a wrong answer.
     *
     * <p>It used to be escaped instead, by wrapping each metacharacter in a one-character class — {@code a*b}
     * became {@code a[*]b}. That made the rewrite depend on class support, and it emitted {@code [[]} for a literal
     * bracket, which the matcher then refused to parse; a filter as ordinary as {@code WHERE dept == 'a[b'} on a
     * Hive dataset produced an uncaught parse failure at query time.
     *
     * <p>Declining is simply better. The rewrite is an optimisation — it narrows which prefix is listed, and the
     * row filter still runs — so skipping it lists a superset, which is always correct. There is no reason to
     * carry an escaping mechanism to save a listing on a partition value nobody writes.
     */
    private static boolean globExpressible(String value) {
        return value.indexOf('*') < 0 && value.indexOf('?') < 0 && value.indexOf('[') < 0 && value.indexOf('{') < 0;
    }

    /** Joins glob-escaped spellings into a brace alternation {@code {a,b,c}}. */
    private static String brace(Set<String> spellings) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (String spelling : spellings) {
            if (first == false) {
                sb.append(',');
            }
            sb.append(spelling);
            first = false;
        }
        return sb.append('}').toString();
    }

    public static List<StorageEntry> applyFileMetadataFilters(List<StorageEntry> entries, List<PartitionFilterHint> hints) {
        List<PartitionFilterHint> fileHints = fileMetadataHints(hints);
        if (fileHints.isEmpty()) {
            return entries;
        }

        int beforeCount = entries.size();
        List<StorageEntry> filtered = new ArrayList<>(entries.size());
        for (StorageEntry entry : entries) {
            if (matchesAllFileHints(entry, fileHints)) {
                filtered.add(entry);
            }
        }

        if (filtered.size() < beforeCount) {
            logger.debug("File metadata filter pruned {}/{} files from listing", beforeCount - filtered.size(), beforeCount);
        }
        return filtered;
    }

    private static boolean matchesAllFileHints(StorageEntry entry, List<PartitionFilterHint> fileHints) {
        for (PartitionFilterHint hint : fileHints) {
            if (matchesFileHint(entry, hint) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesFileHint(StorageEntry entry, PartitionFilterHint hint) {
        return switch (hint.columnName()) {
            case FileMetadataColumns.MODIFIED -> evaluateTimestamp(entry.lastModified(), hint);
            case FileMetadataColumns.SIZE -> evaluateLong(entry.length(), hint);
            case FileMetadataColumns.PATH -> evaluateString(entry.path().toString(), hint);
            case FileMetadataColumns.NAME -> evaluateString(entry.path().objectName(), hint);
            case FileMetadataColumns.DIRECTORY -> {
                StoragePath parent = entry.path().parentDirectory();
                yield parent != null ? evaluateString(parent.toString(), hint) : true;
            }
            default -> true; // Unknown hint — don't filter (safe fallback)
        };
    }

    private static boolean evaluateTimestamp(Instant actual, PartitionFilterHint hint) {
        // StorageEntry normalises a missing lastModified to Instant.EPOCH, so treat both
        // null and EPOCH as "unknown" and let the file pass through rather than
        // accidentally pruning every file whose mtime the store could not provide.
        if (actual == null || actual.equals(Instant.EPOCH)) {
            return true; // Unknown timestamp — don't filter (conservative)
        }
        if (hint.values().isEmpty()) {
            return true;
        }
        long actualMillis = actual.toEpochMilli();

        if (hint.operator() == PartitionFilterHintExtractor.Operator.IN) {
            for (Object v : hint.values()) {
                long millis = toEpochMillis(v);
                if (millis != Long.MIN_VALUE && actualMillis == millis) {
                    return true;
                }
            }
            return false;
        }

        long hintMillis = toEpochMillis(hint.values().get(0));
        if (hintMillis == Long.MIN_VALUE) {
            return true; // Unparseable — don't filter
        }
        return evaluateComparison(Long.compare(actualMillis, hintMillis), hint.operator());
    }

    private static long toEpochMillis(Object value) {
        if (value instanceof Long l) {
            return l;
        } else if (value instanceof String s) {
            try {
                return Instant.parse(s).toEpochMilli();
            } catch (Exception e) {
                return Long.MIN_VALUE;
            }
        }
        return Long.MIN_VALUE;
    }

    private static boolean evaluateLong(long actual, PartitionFilterHint hint) {
        if (hint.values().isEmpty()) {
            return true;
        }
        if (hint.operator() == PartitionFilterHintExtractor.Operator.IN) {
            for (Object v : hint.values()) {
                long inVal;
                if (v instanceof Number n) {
                    inVal = n.longValue();
                } else {
                    try {
                        inVal = Long.parseLong(v.toString());
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }
                if (actual == inVal) {
                    return true;
                }
            }
            return false;
        }

        long hintLong;
        Object hintValue = hint.values().get(0);
        if (hintValue instanceof Number n) {
            hintLong = n.longValue();
        } else if (hintValue instanceof String s) {
            try {
                hintLong = Long.parseLong(s);
            } catch (NumberFormatException e) {
                return true;
            }
        } else {
            return true;
        }
        return evaluateComparison(Long.compare(actual, hintLong), hint.operator());
    }

    private static boolean evaluateString(String actual, PartitionFilterHint hint) {
        if (actual == null || hint.values().isEmpty()) {
            return true;
        }
        if (hint.operator() == PartitionFilterHintExtractor.Operator.IN) {
            for (Object v : hint.values()) {
                if (actual.equals(v.toString())) {
                    return true;
                }
            }
            return false;
        }
        String hintStr = hint.values().get(0).toString();
        return evaluateComparison(actual.compareTo(hintStr), hint.operator());
    }

    private static boolean evaluateComparison(int cmp, PartitionFilterHintExtractor.Operator operator) {
        return switch (operator) {
            case EQUALS -> cmp == 0;
            case NOT_EQUALS -> cmp != 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            case IN -> false; // Handled separately in caller
        };
    }
}
