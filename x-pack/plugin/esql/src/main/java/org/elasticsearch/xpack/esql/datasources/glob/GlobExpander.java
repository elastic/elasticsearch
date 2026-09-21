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
import org.elasticsearch.xpack.esql.core.type.DataType;
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
import org.elasticsearch.xpack.esql.datasources.TemplatePartitionDetector.TemplateSegment;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
     * Notices raised while listing ride on the returned {@link FileList#listingWarnings()}; nothing is emitted here.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        StoragePath storagePath
    ) throws IOException {
        return expandAndCompact(path, provider, hints, config, storagePath, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Expands a glob/comma pattern and compresses the result, with safety caps on discovery.
     * The two-int overload leaves the listing walk uncapped so tests can isolate the kept-files cap.
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
        return expandAndCompact(path, provider, hints, config, storagePath, maxDiscoveredFiles, maxGlobExpansion, Integer.MAX_VALUE);
    }

    /**
     * Expands a glob/comma pattern and compresses the result, with safety caps on kept files, brace expansion,
     * and objects visited while listing.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        StoragePath storagePath,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects
    ) throws IOException {
        return expandAndCompact(
            path,
            provider,
            hints,
            config,
            storagePath,
            maxDiscoveredFiles,
            maxGlobExpansion,
            maxListedObjects,
            Integer.MAX_VALUE
        );
    }

    /**
     * As above, stopping after {@code listingBound} keys have been visited rather than draining the glob.
     * <p>
     * The bound truncates where {@code maxListedObjects} fails: reaching it is the expected outcome, not an error.
     * The result is a prefix of the matching files in listing order, flagged {@link FileList#isTruncated()}, and it
     * is left uncompacted — compaction shrinks large listings and a bounded one is already small. Only a
     * schema-only resolution may pass a bound; {@code Integer.MAX_VALUE} is the unbounded path every reading query
     * takes, byte for byte as before.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        StoragePath storagePath,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects,
        int listingBound
    ) throws IOException {
        FileList expanded = expand(path, provider, hints, config, maxDiscoveredFiles, maxGlobExpansion, maxListedObjects, listingBound);
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
        return expand(path, provider, hints, config, maxDiscoveredFiles, maxGlobExpansion, Integer.MAX_VALUE);
    }

    public static FileList expand(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects
    ) throws IOException {
        return expand(path, provider, hints, config, maxDiscoveredFiles, maxGlobExpansion, maxListedObjects, Integer.MAX_VALUE);
    }

    public static FileList expand(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects,
        int listingBound
    ) throws IOException {
        PartitionConfig partitionConfig = PartitionConfig.fromConfig(config);
        ExclusionConfig.NameFilter nameFilter = ExclusionConfig.fromConfig(config).compile();
        FileOrderConfig fileOrder = FileOrderConfig.forListing(config);
        // A backstop for direct callers of this public entry point, not the decision. The resolver declines the
        // bound for both of these before it gets here, because it must decide BEFORE choosing whether to bypass
        // the listing cache — revoking a bound below that choice leaves a full listing that was never cached,
        // which is worse than not bounding. Repeated here because a bound honoured under either condition picks
        // a different anchor than the unbounded listing would, and this class is reachable without the resolver.
        boolean prefixOfTheWholeGlob = fileOrder.equals(FileOrderConfig.DEFAULT) && hasPartitionPruningHints(hints) == false;
        int effectiveBound = prefixOfTheWholeGlob ? listingBound : Integer.MAX_VALUE;
        // A comma list is several globs; a key budget has no single meaning across them, so it resolves unbounded.
        return isTopLevelCommaList(path)
            ? doExpandCommaSeparated(
                path,
                provider,
                hints,
                partitionConfig,
                maxDiscoveredFiles,
                maxGlobExpansion,
                maxListedObjects,
                nameFilter,
                fileOrder
            )
            : expandGlobWithRewriteFallback(
                path,
                provider,
                hints,
                partitionConfig,
                maxDiscoveredFiles,
                maxGlobExpansion,
                maxListedObjects,
                nameFilter,
                fileOrder,
                effectiveBound
            );
    }

    /**
     * Expands a single glob, re-listing without narrowing if — and only if — a narrowed listing came back empty.
     *
     * <p>Two things narrow a listing, and neither is allowed to decide that a dataset is empty. The glob rewrite
     * ({@link #effectivePattern}/{@link #rewriteSegment}) spells a hint value with {@link String#valueOf}, so
     * {@code WHERE month == 6} narrows the glob to {@code month=6} while the Hive convention writes a zero-padded
     * {@code month=06}. The listing bound keeps only the first keys the provider reports, so a prefix holding
     * nothing but litter or another format matches nothing while the dataset is full of files. Both report empty
     * for a dataset that has data, which is silent zero rows rather than a slow query.
     *
     * <p>So emptiness is decided on the un-narrowed glob: drop the rewrite and the bound and list once more.
     * The {@code _file.*} filters are kept — they are exact and can hide nothing. If that is empty too the pattern
     * genuinely matches nothing and the caller's "matched no files" error stands. A full re-list can exceed
     * {@code max_discovered_files} and throw, exactly as the unfiltered query would; that is deliberate, because
     * telling a narrowing miss from a genuinely empty dataset needs the whole listing.
     *
     * <p>Narrowing is only ever an optimisation: the query's filter still runs on the rows, so listing a superset
     * is always correct while listing a subset is a wrong answer. When nothing narrowed the glob there is nothing
     * to disambiguate and this expands once, with no retry.
     */
    private static FileList expandGlobWithRewriteFallback(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects,
        ExclusionConfig.NameFilter nameFilter,
        FileOrderConfig fileOrder,
        int listingBound
    ) throws IOException {
        boolean rewritten = effectivePattern(pattern, hints, partitionConfig).equals(pattern) == false;
        boolean bounded = listingBound != Integer.MAX_VALUE;
        if (rewritten == false && bounded == false) {
            return doExpandGlob(
                pattern,
                provider,
                hints,
                partitionConfig,
                maxDiscoveredFiles,
                maxGlobExpansion,
                maxListedObjects,
                nameFilter,
                fileOrder,
                Integer.MAX_VALUE
            );
        }

        FileList narrowed = null;
        // A rewritten prefix may name a folder that does not exist; the local filesystem throws where object stores
        // return empty. That is the rewrite failing, not the dataset, so it takes the same path as an empty result.
        IOException failure = null;
        try {
            narrowed = doExpandGlob(
                pattern,
                provider,
                hints,
                partitionConfig,
                maxDiscoveredFiles,
                maxGlobExpansion,
                maxListedObjects,
                nameFilter,
                fileOrder,
                listingBound
            );
        } catch (IOException e) {
            failure = e;
        }
        if (failure == null && (narrowed.isResolved() == false || narrowed.fileCount() > 0)) {
            return narrowed;
        }
        // Only the rewrite can throw spuriously: it may name a folder that does not exist, and the local
        // filesystem throws there where object stores return empty. A bound cannot invent an IOException, so when
        // the bound was the only narrowing the error is the storage's own — transient or not — and re-listing the
        // whole glob would answer a schema request with the full enumeration the bound exists to avoid, and would
        // bury the original error if it then succeeded. Surface it instead.
        if (failure != null && rewritten == false) {
            throw failure;
        }

        final IOException narrowedFailure = failure;
        logger.debug(
            () -> "Narrowed listing of [" + pattern + "] yielded no files; re-listing without the narrowing that produced it",
            narrowedFailure
        );
        try {
            return doExpandGlob(
                pattern,
                provider,
                // The rewrite is dropped; the exact _file.* filters are kept.
                rewritten ? fileMetadataHints(hints) : hints,
                partitionConfig,
                maxDiscoveredFiles,
                maxGlobExpansion,
                maxListedObjects,
                nameFilter,
                fileOrder,
                Integer.MAX_VALUE
            );
        } catch (IOException retryFailure) {
            if (failure != null) {
                retryFailure.addSuppressed(failure);
            }
            throw retryFailure;
        }
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
     *
     * For {@code http}/{@code https}, commas are never treated as list separators — the scheme does
     * not support glob expansion or multi-resource lists. {@link StoragePath} strips the query string
     * from the path for those schemes, so metacharacters in the query are invisible to
     * {@link StoragePath#isPattern()}.
     */
    public static boolean isMultiFile(String path) {
        if (path == null) {
            return false;
        }
        try {
            StoragePath sp = StoragePath.of(path);
            // For http/https the query string is stripped by StoragePath.of(), so isPattern()
            // sees only the resource path. Commas and glob metacharacters in the query are
            // structural URL delimiters, not list separators or wildcards.
            if (sp.scheme().equalsIgnoreCase("http") || sp.scheme().equalsIgnoreCase("https")) {
                return sp.isPattern();
            }
            return hasTopLevelComma(path) || sp.isPattern();
        } catch (IllegalArgumentException e) {
            // Not a parseable URL; fall back to scanning the whole string
            return hasTopLevelComma(path) || StoragePath.containsGlobMetacharacter(path);
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
        FileOrderConfig fileOrder = FileOrderConfig.forListing(config);
        return doExpandGlob(
            pattern,
            provider,
            hints,
            PartitionConfig.fromConfig(config),
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            nameFilter,
            fileOrder,
            Integer.MAX_VALUE
        );
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        return expandGlob(pattern, provider, hints, config, maxDiscoveredFiles, maxGlobExpansion, Integer.MAX_VALUE);
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects
    ) throws IOException {
        ExclusionConfig.NameFilter nameFilter = ExclusionConfig.fromConfig(config).compile();
        FileOrderConfig fileOrder = FileOrderConfig.forListing(config);
        return doExpandGlob(
            pattern,
            provider,
            hints,
            PartitionConfig.fromConfig(config),
            maxDiscoveredFiles,
            maxGlobExpansion,
            maxListedObjects,
            nameFilter,
            fileOrder,
            Integer.MAX_VALUE
        );
    }

    static FileList doExpandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects,
        ExclusionConfig.NameFilter nameFilter,
        FileOrderConfig fileOrder,
        int listingBound
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
                List<String> notices = new ArrayList<>();
                PartitionMetadata partitionMetadata = detectPartitions(List.of(entry), partitionConfig, notices::add);
                return new GenericFileList(List.of(entry), pattern, partitionMetadata, notices);
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
        List<PartitionFilterHint> fileHints = fileMetadataHints(hints);

        // Enumerable pattern: probe each key with exists() instead of listing a prefix that may hold millions.
        List<String> candidates = matcher.enumerateKeys(maxGlobExpansion);
        if (candidates != null) {
            List<StorageEntry> matched = new ArrayList<>();
            StorageEntry fileHintAnchor = null;
            String prefixStr = prefix.toString();
            for (String candidate : candidates) {
                StoragePath fullPath = StoragePath.of(prefixStr + candidate);
                var obj = provider.newObject(fullPath);
                if (obj.exists()) {
                    StorageEntry entry = new StorageEntry(fullPath, obj.length(), obj.lastModified());
                    fileHintAnchor = addOrStashAnchor(entry, fileHints, matched, fileHintAnchor, maxDiscoveredFiles);
                }
            }
            if (matched.isEmpty() && fileHintAnchor != null && maxDiscoveredFiles > 0) {
                matched.add(fileHintAnchor);
            }
            if (matched.isEmpty()) {
                return FileList.EMPTY;
            }
            fileOrder.apply(matched);
            List<String> notices = new ArrayList<>();
            PartitionMetadata partitionMetadata = detectPartitions(matched, partitionConfig, notices::add);
            return new GenericFileList(matched, pattern, partitionMetadata, notices);
        }

        boolean recursive = matcher.needsRecursion();

        // A glob leading with the recursive wildcard names no partition key the textual rewrite could act on, so
        // the walk narrows the enumeration itself. Every declined or failed shape falls through to the flat listing
        // below; see PartitionPruningWalk for the fail-closed rules and the trust boundary.
        // A bounded listing skips the walk. The walk narrows by descending the partition tree, which costs several
        // requests; the flat path under a bound costs one page and is what the bound was asked for.
        if (listingBound == Integer.MAX_VALUE && globstarLeads(glob) && walkableStrategy(partitionConfig)) {
            List<PartitionFilterHint> partitionHints = partitionPruningHints(hints);
            if (partitionHints.isEmpty() == false) {
                PartitionPruningWalk.WalkResult walk = PartitionPruningWalk.tryWalk(
                    provider,
                    prefix,
                    matcher,
                    nameFilter,
                    partitionHints,
                    maxDiscoveredFiles
                );
                // An all-pruned walk mirrors the rewrite-to-empty fallback: re-list flat so the resolver keeps a
                // schema-inference anchor; the row filter still yields zero matching rows.
                if (walk != null && walk.matched().isEmpty() == false) {
                    List<StorageEntry> walked = walk.matched();
                    if (fileHints.isEmpty() == false) {
                        List<StorageEntry> filtered = new ArrayList<>();
                        StorageEntry fileHintAnchor = null;
                        for (StorageEntry entry : walked) {
                            fileHintAnchor = addOrStashAnchor(entry, fileHints, filtered, fileHintAnchor, maxDiscoveredFiles);
                        }
                        if (filtered.isEmpty() && fileHintAnchor != null && maxDiscoveredFiles > 0) {
                            filtered.add(fileHintAnchor);
                        }
                        walked = filtered;
                    }
                    fileOrder.apply(walked);
                    List<String> walkNotices = new ArrayList<>();
                    PartitionMetadata walkedMetadata = detectPartitions(walked, partitionConfig, walkNotices::add);
                    if (walkPruningProven(walk.prunedColumns(), walkedMetadata)) {
                        if (walkTypesConsistent(walk, walkedMetadata)) {
                            // Counted pre-_file.*-filter, as the flat path counts.
                            if (walk.excludedCount() > 0) {
                                walkNotices.add(
                                    exclusionWarning(
                                        walk.excludedCount(),
                                        walk.matched().size(),
                                        prefix.toString(),
                                        walk.excludedExample(),
                                        walk.excludedExampleEntry()
                                    )
                                );
                            }
                            return new GenericFileList(walked, pattern, walkedMetadata, walkNotices);
                        }
                        logger.debug("Walked listing of [{}] would narrow the type of partition column(s); re-listing flat", pattern);
                    } else {
                        logger.debug(
                            "Walked listing of [{}] does not detect the pruned-on partition columns {}; re-listing flat",
                            pattern,
                            walk.prunedColumns()
                        );
                    }
                }
            }
        }

        List<StorageEntry> matched = new ArrayList<>();
        StorageEntry fileHintAnchor = null;
        String prefixStr = prefix.toString();
        // One warning per listing, however many objects it drops. The counts are the useful part: how many of the
        // objects the resource selected were then dropped tells the user whether they are missing a stray marker or
        // most of their data. Enumerating them would emit a header per partition on a prefix with a marker in each.
        int excludedCount = 0;
        // Exclusion warning totals are glob matches before _file.* prune, same as the pre-filter count.
        int globKeptCount = 0;
        String excludedExample = null;
        String excludedExampleEntry = null;
        int listed = 0;

        // Set when the drain stopped at listingBound rather than exhausting the glob.
        boolean truncated = false;
        try (StorageIterator iterator = provider.listObjects(prefix, recursive)) {
            while (iterator.hasNext()) {
                if (listed >= listingBound) {
                    // Every provider's iterator fetches a page only when the current one is exhausted, so
                    // returning here is what makes the bound an I/O saving rather than a filter over keys we
                    // already paid to read.
                    truncated = true;
                    break;
                }
                StorageEntry entry = iterator.next();
                listed++;
                checkListedObjectsLimit(listed, maxListedObjects);
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
                if (matcher.matches(relativePath)) {
                    String excludedBy = nameFilter.excludedBy(relativePath);
                    if (excludedBy == null) {
                        globKeptCount++;
                        fileHintAnchor = addOrStashAnchor(entry, fileHints, matched, fileHintAnchor, maxDiscoveredFiles);
                    } else {
                        // Matched what the user asked for and was dropped anyway. Keep the first one so the warning
                        // can name a concrete file and the entry responsible; "some files were excluded" on its own
                        // leaves the user with nothing to act on.
                        excludedCount++;
                        if (excludedExample == null) {
                            excludedExample = relativePath;
                            excludedExampleEntry = excludedBy;
                        }
                    }
                }
            }
        }

        List<String> listingWarnings = new ArrayList<>();
        if (excludedCount > 0) {
            listingWarnings.add(exclusionWarning(excludedCount, globKeptCount, prefixStr, excludedExample, excludedExampleEntry));
        }

        if (matched.isEmpty() && fileHintAnchor != null && maxDiscoveredFiles > 0) {
            matched.add(fileHintAnchor);
        }

        if (matched.isEmpty()) {
            // FileList.EMPTY is a shared sentinel and cannot carry per-listing warnings. Litter-only
            // prefixes still need the exclusion text on a cacheable empty listing.
            // Carries `truncated` even when nothing matched: the shared EMPTY sentinel cannot hold it, so a
            // bounded empty listing takes the GenericFileList branch whether or not there are warnings.
            return listingWarnings.isEmpty() && truncated == false
                ? FileList.EMPTY
                : new GenericFileList(List.of(), pattern, null, listingWarnings, truncated);
        }

        fileOrder.apply(matched);

        PartitionMetadata partitionMetadata = detectPartitions(matched, partitionConfig, listingWarnings::add);

        return new GenericFileList(matched, pattern, partitionMetadata, listingWarnings, truncated);
    }

    /**
     * One warning per listing, however many objects it drops. Counted against everything the resource
     * pattern selected (kept plus dropped). Fires for the default exclusion list too: a user who never
     * configured exclusion cannot guess why a visible object is missing from results.
     */
    private static String exclusionWarning(
        int excludedCount,
        int matchedCount,
        String prefix,
        String excludedExample,
        String excludedExampleEntry
    ) {
        return excludedCount
            + " of "
            + (matchedCount + excludedCount)
            + " objects matching the resource under ["
            + prefix
            + (excludedCount == 1 ? "] was excluded by the [" : "] were excluded by the [")
            + ExclusionConfig.CONFIG_FILE_EXCLUSIONS
            + "] dataset setting, for example ["
            + excludedExample
            + "] which matched entry ["
            + excludedExampleEntry
            + "]";
    }

    /**
     * Mutates {@code matched}: appends {@code entry} when there are no {@code _file.*} hints or it matches them,
     * then returns {@code anchor} unchanged. Otherwise leaves {@code matched} alone and returns {@code anchor} if
     * set, else this reject, as the schema-inference stash. The discovered-files cap fires only on a kept file, so a
     * {@code _file.*} filter can hold the kept set under the cap while listing continues. An all-pruned result is
     * genuinely zero rows, but the resolver needs one file to infer schema; the caller promotes a stashed anchor when
     * {@code matched} is empty. That also keeps a genuine {@code _file.*} miss out of
     * {@link #expandGlobWithRewriteFallback}'s rewrite-only retry.
     * <p>
     * One stashed file is intentional. The previous post-filter path returned every pre-filter match when
     * {@code _file.*} emptied the list, which would put those files back under {@code max_discovered_files} and
     * undo the cap split. Union across pruned files that contribute no rows is not worth holding the full glob.
     * The donor is the first listing-order reject, not a {@code file_order} pick over the pre-filter set.
     */
    private static StorageEntry addOrStashAnchor(
        StorageEntry entry,
        List<PartitionFilterHint> fileHints,
        List<StorageEntry> matched,
        @Nullable StorageEntry anchor,
        int maxDiscoveredFiles
    ) {
        if (fileHints.isEmpty() || matchesAllFileHints(entry, fileHints)) {
            matched.add(entry);
            checkDiscoveredFilesLimit(matched.size(), maxDiscoveredFiles);
            return anchor;
        }
        return anchor != null ? anchor : entry;
    }

    /**
     * The partition columns a listing carries, decided entirely by the resolved {@link PartitionConfig}. One input,
     * one decision: no separate enable flag and no raw settings map alongside it.
     */
    static PartitionMetadata detectPartitions(List<StorageEntry> files, PartitionConfig partitionConfig, Consumer<String> warningSink) {
        if (PartitionConfig.Strategy.NONE == partitionConfig.strategy()) {
            return null;
        }
        PartitionDetector detector = resolveDetector(partitionConfig);
        if (detector == null) {
            return null;
        }
        PartitionMetadata result = detector.detect(files, warningSink);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return result;
    }

    /** Whether the glob's first segment is the recursive wildcard — the shape the partition-pruning walk narrows. */
    private static boolean globstarLeads(String glob) {
        return glob.equals("**") || glob.startsWith("**/");
    }

    /**
     * Whether the resolved strategy licenses matching {@code key=value} folders during the listing walk: {@code HIVE}
     * always; {@code AUTO} only without a usable template (then it is Hive-or-nothing — with one, detection could
     * resolve to template columns the walk knows nothing about). {@code TEMPLATE} binds whole segments and
     * {@code NONE} has no partition columns; neither may prune.
     */
    private static boolean walkableStrategy(PartitionConfig config) {
        return switch (config.strategy()) {
            case HIVE -> true;
            case AUTO -> config.pathTemplate() == null || TemplatePartitionDetector.parseTemplateColumns(config.pathTemplate()).isEmpty();
            case TEMPLATE, NONE -> false;
        };
    }

    /**
     * A folder prune is trusted only when the pruned listing itself detects the pruned-on column as a partition
     * column. A stray file outside the {@code key=value} structure breaks detection and makes the column a data
     * column whose values could live anywhere; this check turns that from silently dropped rows into a flat
     * re-listing. Passes only on the pruned columns; non-pruned column types are verified by
     * {@link #walkTypesConsistent}.
     */
    private static boolean walkPruningProven(Set<String> prunedColumns, @Nullable PartitionMetadata metadata) {
        if (prunedColumns.isEmpty()) {
            return true;
        }
        return metadata != null && metadata.partitionColumns().keySet().containsAll(prunedColumns);
    }

    /**
     * Verifies that the walk did not narrow the type of any non-pruned partition column relative to what the full
     * value set (including values inside pruned subtrees) would produce. A pruned subtree may be the sole source of
     * a type-widening folder value for another column: e.g. {@code year=2023/month=abc} (widening {@code month} to
     * keyword) pruned by {@code year >= 2024} — the walked listing sees only {@code month=06} and detects
     * {@code month} as integer, while the flat listing would detect it as keyword. No file is dropped, but the
     * declared schema would differ. The walk addresses this by peeking one level into pruned dirs to capture shadow
     * values (see {@code PartitionPruningWalk}); this method checks whether those shadow values change any
     * column's inferred type — including pruned columns, whose walked type may narrow when only matching
     * folders survive (e.g. {@code month=06} is INTEGER alone but KEYWORD with {@code month=abc} present).
     *
     * <p><b>Residual limitation.</b> The peek is one level deep: if the type-widening value is more than one
     * level inside the pruned subtree (e.g. {@code a=1/b=x/month=abc} pruned at {@code a}), the divergence in
     * {@code month}'s type is not detected. Such cases are unusual (consistent partition layouts rarely vary type
     * across different parent subtrees) and a flat re-listing is the safe fallback for any undetected case.
     */
    private static boolean walkTypesConsistent(PartitionPruningWalk.WalkResult walk, @Nullable PartitionMetadata metadata) {
        if (metadata == null || walk.prunedColumns().isEmpty()) {
            return true;
        }
        Map<String, DataType> fullTypes = walk.columnFullTypes();
        for (Map.Entry<String, DataType> e : metadata.partitionColumns().entrySet()) {
            DataType fullType = fullTypes.get(e.getKey());
            if (fullType != null && fullType != e.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * The hints that may prune {@code key=value} folders during the listing walk: every non-{@code _file.*} filter
     * column. Also the exact hint set the cache key carries for a walk-eligible pattern — see {@link ListingIdentity}.
     */
    /**
     * Whether these hints select a subtree of the dataset rather than filtering files by their own metadata.
     * <p>
     * A listing bound keeps the first keys the provider reports, which is only a prefix of the same listing when
     * nothing else narrows it. Partition pruning does narrow it — {@link PartitionPruningWalk} descends only the
     * directories a hint admits — and the flat listing applies no partition pruning at all, since the hints it
     * consults ({@link #fileMetadataHints}) are the complement of these. So a bounded listing and an unbounded one
     * over the same hinted glob enumerate different files, not a prefix and its whole, and would disagree about
     * which file is first. Callers that must preserve the anchor use this to decline the bound.
     */
    public static boolean hasPartitionPruningHints(@Nullable List<PartitionFilterHint> hints) {
        return partitionPruningHints(hints).isEmpty() == false;
    }

    static List<PartitionFilterHint> partitionPruningHints(@Nullable List<PartitionFilterHint> hints) {
        if (hints == null || hints.isEmpty()) {
            return List.of();
        }
        List<PartitionFilterHint> partitionHints = new ArrayList<>();
        for (PartitionFilterHint hint : hints) {
            if (FileMetadataColumns.isFileMetadataColumn(hint.columnName()) == false && hint.values().isEmpty() == false) {
                partitionHints.add(hint);
            }
        }
        return partitionHints;
    }

    /**
     * Aborts when a listing kept more files than {@code maxDiscoveredFiles}. Public so a cached
     * listing can be re-checked after a live cap drop without expanding again.
     */
    public static void checkDiscoveredFilesLimit(int discoveredCount, int maxDiscoveredFiles) {
        Check.clientError(
            discoveredCount <= maxDiscoveredFiles,
            "Glob pattern discovered too many files ({}, limit {}). Narrow your glob pattern, add partition "
                + "filters, or increase the [esql.external.max_discovered_files] cluster setting.",
            discoveredCount,
            maxDiscoveredFiles
        );
    }

    private static void checkListedObjectsLimit(int listedCount, int maxListedObjects) {
        Check.clientError(
            listedCount <= maxListedObjects,
            "Glob pattern listed too many objects ({}, limit {}). Narrow your glob pattern, add partition "
                + "filters, or increase the [esql.external.max_listed_objects] cluster setting.",
            listedCount,
            maxListedObjects
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
            Integer.MAX_VALUE,
            ExclusionConfig.fromConfig(config).compile(),
            FileOrderConfig.forListing(config)
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
        return expandCommaSeparated(pathList, provider, hints, config, maxDiscoveredFiles, maxGlobExpansion, Integer.MAX_VALUE);
    }

    public static FileList expandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects
    ) throws IOException {
        return doExpandCommaSeparated(
            pathList,
            provider,
            hints,
            PartitionConfig.fromConfig(config),
            maxDiscoveredFiles,
            maxGlobExpansion,
            maxListedObjects,
            ExclusionConfig.fromConfig(config).compile(),
            FileOrderConfig.forListing(config)
        );
    }

    private static FileList doExpandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        PartitionConfig partitionConfig,
        int maxDiscoveredFiles,
        int maxGlobExpansion,
        int maxListedObjects,
        ExclusionConfig.NameFilter nameFilter,
        FileOrderConfig fileOrder
    ) throws IOException {
        Check.notNull(pathList, "pathList cannot be null");
        Check.notNull(provider, "provider cannot be null");

        List<StorageEntry> allEntries = new ArrayList<>();
        List<String> listingWarnings = new ArrayList<>();

        for (String trimmed : commaSegments(pathList)) {
            StoragePath segmentPath = StoragePath.of(trimmed);
            if (segmentPath.isPattern()) {
                int remainingBudget = maxDiscoveredFiles - allEntries.size();
                if (remainingBudget <= 0) {
                    // Earlier segments already filled the kept-files cap. Expanding further would only
                    // walk, or turn a rewrite-hit _file.* miss into a fallback that throws limit 0.
                    continue;
                }
                // Per segment, so a segment a rewrite narrows to empty falls back on its own instead of being masked
                // by another segment that still matches. Each glob gets the same walk cap: FileList does not report
                // how many keys were listed, so there is no remaining listed-objects budget to share.
                FileList expanded = expandGlobWithRewriteFallback(
                    trimmed,
                    provider,
                    hints,
                    partitionConfig,
                    remainingBudget,
                    maxGlobExpansion,
                    maxListedObjects,
                    nameFilter,
                    // Discovery order only. fileOrder is applied once on the concatenated list so
                    // list+desc is reverse(concat) rather than reverse(concat(reverse(g1), reverse(g2))).
                    FileOrderConfig.DEFAULT,
                    // A key budget has no single meaning across the segments of a comma list, so each
                    // segment lists in full; expand() never hands this path a bound.
                    Integer.MAX_VALUE
                );
                listingWarnings.addAll(expanded.listingWarnings());
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
            return listingWarnings.isEmpty() ? FileList.EMPTY : new GenericFileList(List.of(), pathList, null, listingWarnings);
        }

        fileOrder.apply(allEntries);

        PartitionMetadata partitionMetadata = detectPartitions(allEntries, partitionConfig, listingWarnings::add);

        return new GenericFileList(allEntries, pathList, partitionMetadata, listingWarnings);
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
     * {@link PartitionConfig} (strategy AND path template), the effective (post-rewrite) glob pattern, the
     * {@code _file.*} metadata filters, the partition hints when the effective pattern is walk-eligible (see
     * {@link PartitionPruningWalk}), the resolved {@link ExclusionConfig}, and the resolved {@link FileOrderConfig}.
     * These are the inputs {@link #doExpandGlob} consults beyond the storage contents themselves — via
     * {@link #effectivePattern}, {@link #applyFileMetadataFilters} and {@link #partitionPruningHints} — and this
     * value shares those same helpers, so the listing cache key cannot drift from the listing it names. It binds only
     * the cache key: a new
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
        List<String> encodedPartitionHints,
        ExclusionConfig exclusionConfig,
        FileOrderConfig fileOrder
    ) {

        static ListingIdentity of(
            String path,
            @Nullable List<PartitionFilterHint> hints,
            PartitionConfig partitionConfig,
            ExclusionConfig exclusionConfig,
            FileOrderConfig fileOrder
        ) {
            String effectivePattern = effectiveWholePathPattern(path, hints, partitionConfig);
            return new ListingIdentity(
                partitionConfig,
                effectivePattern,
                encodedHints(fileMetadataHints(hints)),
                // The walk is the second hint channel into the listing: partition hints decide which folders are
                // enumerated without changing the effective pattern, so on a walk-eligible pattern they must join
                // the identity or a filtered query poisons the cache. Eligibility is judged on the EFFECTIVE
                // pattern — a keyed data/year=*/** rewrites to data/year=2024/** and the walk prunes under that
                // prefix. Provider support cannot be known at key time; over-inclusion merely fragments, safely.
                walkShapeEligible(effectivePattern, partitionConfig) ? encodedHints(partitionPruningHints(hints)) : List.of(),
                exclusionConfig,
                fileOrder
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
            sb.append(encodedPartitionHints.size()).append(':');
            for (String encodedHint : encodedPartitionHints) {
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
            // Compacted listings bake file order; flipping file_sort_by or file_order must miss the cache even when
            // the discovered set is identical. Fixed vocabulary, still length-prefixed so the framing
            // stays uniform with every other field.
            appendLengthPrefixed(sb, fileOrder.sortBy().name());
            appendLengthPrefixed(sb, fileOrder.order().name());
            return sb.toString();
        }
    }

    /**
     * Whether any glob of {@code effectivePattern} (the lone post-rewrite pattern, or a comma segment of it) has the
     * shape and strategy the walk acts on. Ignores whether the walk would actually prune — the identity only needs
     * to be at least as fine-grained as the listing decision it names.
     */
    private static boolean walkShapeEligible(String effectivePattern, PartitionConfig partitionConfig) {
        if (walkableStrategy(partitionConfig) == false) {
            return false;
        }
        List<String> segments = hasTopLevelComma(effectivePattern) ? commaSegments(effectivePattern) : List.of(effectivePattern);
        for (String segment : segments) {
            try {
                StoragePath storagePath = StoragePath.of(segment);
                if (storagePath.isPattern() && globstarLeads(storagePath.globPart())) {
                    return true;
                }
            } catch (IllegalArgumentException e) {
                // Unparseable segment: the expansion that follows raises the error; it cannot be walk-eligible.
            }
        }
        return false;
    }

    /** Appends {@code <charLength>':'<value>}, an injective framing that no value content can forge a boundary in. */
    private static void appendLengthPrefixed(StringBuilder sb, String value) {
        sb.append(value.length()).append(':').append(value);
    }

    /**
     * A string that identifies the listing a given set of hints produces for a given path: equal discriminators
     * guarantee equal listings, so it is safe to key the listing cache on it. See {@link ListingIdentity} for the
     * inputs and why they are exhaustive; hints that reach none of them leave the discriminator untouched, so an
     * incidentally-filtered query still shares the un-filtered entry. On a walk-eligible pattern every
     * non-{@code _file.*} hint joins the key — pre-resolution nothing can tell a partition column from a data
     * column, so over-inclusion (safe fragmentation) is the only sound reading. The exclusion settings resolve from
     * {@code config} via {@link ExclusionConfig#fromConfig}. File order resolves via {@link FileOrderConfig#forListing}.
     */
    public static String listingCacheDiscriminator(
        String path,
        @Nullable List<PartitionFilterHint> hints,
        @Nullable Map<String, Object> config
    ) {
        return ListingIdentity.of(
            path,
            hints,
            PartitionConfig.fromConfig(config),
            ExclusionConfig.fromConfig(config),
            FileOrderConfig.forListing(config)
        ).encode();
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
        if (isTopLevelCommaList(path) == false) {
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
     * ({@link #doExpandCommaSeparated}), the identity that names its result ({@link #effectiveWholePathPattern}), and
     * the local-disk allowlist gate ({@code LocalFileAccess#check}), so none of them can disagree on which segments a
     * path has. A single-file (or brace-only) path yields exactly one segment, so callers can treat {@code size() > 1}
     * as "this is a multi-file listing".
     */
    public static List<String> commaSegments(String pathList) {
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

    /**
     * Ceiling matching {@link GlobMatcher}'s per-group cap so format inference cannot expand a wider set than
     * listing would.
     */
    private static final int MAX_BRACE_ALTERNATIVES = 1024;

    /**
     * Expands brace groups in an object name while keeping {@code *}, {@code ?}, and {@code [} as themselves.
     * Used to infer the formats a resource pattern implies without listing objects — {@link GlobMatcher#enumerateKeys}
     * returns null when a wildcard is present, which would hide {@code *.{parquet,csv}}.
     *
     * <p>Nested or unterminated brace groups, and numeric ranges that cannot be expanded, are left as the original
     * spelling so the caller treats them as unreadable names rather than inventing a format.
     */
    public static List<String> expandBracesKeepingWildcards(String name) {
        if (name == null) {
            return List.of();
        }
        if (name.isEmpty() || name.indexOf('{') < 0) {
            return List.of(name);
        }
        List<String> out = new ArrayList<>();
        expandBracesKeepingWildcards(name, out);
        // Incomplete expansion of a huge group would look like a unique format. Fall back to the
        // original spelling so inference treats it as unreadable rather than silently picking a subset.
        if (out.size() > MAX_BRACE_ALTERNATIVES) {
            return List.of(name);
        }
        return out;
    }

    private static void expandBracesKeepingWildcards(String name, List<String> out) {
        int open = name.indexOf('{');
        if (open < 0) {
            out.add(name);
            return;
        }
        int close = name.indexOf('}', open + 1);
        if (close < 0) {
            out.add(name);
            return;
        }
        String body = name.substring(open + 1, close);
        if (body.indexOf('{') >= 0) {
            out.add(name);
            return;
        }
        String[] spellings = BraceExpander.expandBraceContent(body, MAX_BRACE_ALTERNATIVES);
        if (spellings == null) {
            out.add(name);
            return;
        }
        String prefix = name.substring(0, open);
        String suffix = name.substring(close + 1);
        for (String spelling : spellings) {
            expandBracesKeepingWildcards(prefix + spelling + suffix, out);
        }
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
     * Whether {@code path} is a comma-separated list of resources (as opposed to a single glob or literal).
     * For {@code http}/{@code https}, commas are never treated as list separators — the scheme does not
     * support glob expansion or multi-resource lists. For all other schemes, delegates to
     * {@link #hasTopLevelComma}.
     */
    private static boolean isTopLevelCommaList(String path) {
        try {
            StoragePath sp = StoragePath.of(path);
            if (sp.scheme().equalsIgnoreCase("http") || sp.scheme().equalsIgnoreCase("https")) {
                return false;
            }
        } catch (IllegalArgumentException e) {
            // Not a parseable URL — treat as non-HTTP and fall through
        }
        return hasTopLevelComma(path);
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

    /** A hint list encoded injectively (each field length-prefixed) and ordered, for use in a cache key. */
    private static List<String> encodedHints(List<PartitionFilterHint> hints) {
        if (hints.isEmpty()) {
            return List.of();
        }
        List<String> encoded = new ArrayList<>(hints.size());
        for (PartitionFilterHint hint : hints) {
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
        List<TemplateSegment> templateSegments = TemplatePartitionDetector.parseTemplate(template);
        if (hasPlaceholder(templateSegments) == false) {
            return null;
        }

        StoragePath parsed;
        try {
            parsed = StoragePath.of(pattern);
        } catch (IllegalArgumentException e) {
            return null;
        }
        String path = parsed.path();
        if (path == null || path.isEmpty() || pattern.endsWith(path) == false) {
            return null;
        }

        // Edit a path()-shaped split (leading empty and filename kept). Joining directorySegments
        // would drop the leading slash and smash the authority: s3://bucket + logs/... = s3://bucketlogs/...
        String[] raw = path.split("/");
        List<Integer> nonEmptyIdx = new ArrayList<>();
        for (int i = 0; i < raw.length; i++) {
            if (raw[i].isEmpty() == false) {
                nonEmptyIdx.add(i);
            }
        }
        if (nonEmptyIdx.size() < templateSegments.size() + 1) {
            return null;
        }
        List<Integer> dirIdx = nonEmptyIdx.subList(0, nonEmptyIdx.size() - 1);
        int offset = dirIdx.size() - templateSegments.size();
        boolean changed = false;
        for (int t = 0; t < templateSegments.size(); t++) {
            int rawIdx = dirIdx.get(offset + t);
            String globSlot = raw[rawIdx];
            switch (templateSegments.get(t)) {
                case TemplateSegment.Literal(String value) -> {
                    if ("*".equals(globSlot)) {
                        // A leftover * lists sibling directories that fail extractByTemplate and
                        // drop partition detection for the whole batch. Pin the required name.
                        raw[rawIdx] = value;
                        changed = true;
                    } else if (value.equals(globSlot) == false) {
                        return null;
                    }
                }
                case TemplateSegment.Placeholder(String name) -> {
                    if ("*".equals(globSlot) == false) {
                        return null;
                    }
                    PartitionFilterHint hint = rewritableHints.get(name);
                    if (hint == null) {
                        continue;
                    }
                    List<Object> values = hint.values();
                    if (hint.isSingleValue()) {
                        String value = String.valueOf(values.get(0));
                        if (globExpressible(value) == false) {
                            continue;
                        }
                        raw[rawIdx] = value;
                    } else {
                        Set<String> spellings = partitionValueSpellings(values);
                        if (braceExpressible(spellings) == false) {
                            continue;
                        }
                        raw[rawIdx] = brace(spellings);
                    }
                    changed = true;
                }
            }
        }

        if (changed == false) {
            return null;
        }
        String newPath = String.join("/", raw);
        return pattern.substring(0, pattern.length() - path.length()) + newPath;
    }

    private static boolean hasPlaceholder(List<TemplateSegment> segments) {
        for (TemplateSegment segment : segments) {
            if (segment instanceof TemplateSegment.Placeholder) {
                return true;
            }
        }
        return false;
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
