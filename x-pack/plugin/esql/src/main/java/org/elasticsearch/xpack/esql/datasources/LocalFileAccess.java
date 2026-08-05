/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Path;
import java.util.List;

/**
 * Enforces the {@code esql.datasource.local_allowed_paths} allowlist gate for {@code file://} external sources.
 *
 * <p>An empty allowlist (the default) disables local-disk access entirely — the list <em>is</em> the enable,
 * mirroring {@code path.repo} / {@code FsRepository}. When non-empty, a {@code file://} path is accepted only if
 * it normalizes to a location under one of the configured roots; {@code ..}-escape traversals and paths outside
 * every root are rejected.
 *
 * <p>Use {@link #create(Settings)} at node startup. The special singleton {@link #UNRESTRICTED} bypasses all
 * checks and is intended solely for test-only constructors.
 */
public class LocalFileAccess {

    /**
     * Error shown when a {@code file://} read is attempted but local-disk access is disabled (empty allowlist).
     * Shared between the coordinator-side check ({@link FileSourceFactory}) and the data-node-side check
     * ({@link StorageProviderRegistry}) so both paths report the same message.
     */
    public static final String LOCAL_DISK_DISABLED_MESSAGE = "local filesystem access via file:// is disabled; "
        + "set the [esql.datasource.local_allowed_paths] node setting to one or more allowed root paths to enable it";

    /**
     * Error shown when a {@code file://} path does not fall under any allowed root (including {@code ..}-escape
     * attempts). Follows the pattern of the analogous {@code FsRepository} message.
     */
    static final String PATH_OUTSIDE_ALLOWLIST_PREFIX =
        "location doesn't match any of the local paths specified by [esql.datasource.local_allowed_paths]: ";

    /**
     * Allow-all sentinel for test-only constructors in {@link StorageProviderRegistry}, {@link FileSourceFactory},
     * and {@link DataSourceModule}. Does not apply path confinement. Never use in production.
     */
    public static final LocalFileAccess UNRESTRICTED = new LocalFileAccess(true, new Path[0]) {
        @Override
        public void check(StoragePath path) { /* unrestricted */ }

        @Override
        public void check(String location) { /* unrestricted */ }
    };

    private final boolean enabled;
    private final Path[] allowedRoots;

    private LocalFileAccess(boolean enabled, Path[] allowedRoots) {
        this.enabled = enabled;
        this.allowedRoots = allowedRoots;
    }

    /**
     * Builds a {@code LocalFileAccess} from the node's startup settings.
     * An empty {@link ExternalSourceSettings#LOCAL_ALLOWED_PATHS} list (the default) disables local-disk reads.
     */
    @SuppressForbidden(reason = "LocalFileAccess converts configured path strings to normalized absolute Path roots")
    public static LocalFileAccess create(Settings settings) {
        List<String> rawPaths = ExternalSourceSettings.LOCAL_ALLOWED_PATHS.get(settings);
        if (rawPaths.isEmpty()) {
            return new LocalFileAccess(false, new Path[0]);
        }
        Path[] roots = new Path[rawPaths.size()];
        for (int i = 0; i < rawPaths.size(); i++) {
            roots[i] = PathUtils.get(rawPaths.get(i)).toAbsolutePath().normalize();
        }
        return new LocalFileAccess(true, roots);
    }

    /** Returns {@code true} when local-disk reads are permitted (non-empty allowlist). */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Validates that the given {@link StoragePath} is permitted.
     *
     * <p>No-op for non-{@code file} schemes. For {@code file://}:
     * <ul>
     *   <li>If disabled (empty allowlist): throws {@link IllegalArgumentException} with
     *       {@link #LOCAL_DISK_DISABLED_MESSAGE}.</li>
     *   <li>If enabled: resolves the path against the allowed roots via
     *       {@link PathUtils#get(Path[], String)} (lexical normalize + {@code startsWith} — same as
     *       {@code FsRepository} / {@code Environment.resolveRepoDir}). A {@code null} result means the path falls
     *       outside every allowed root (including {@code ..}-escapes); throws with the location appended to
     *       {@link #PATH_OUTSIDE_ALLOWLIST_PREFIX}.</li>
     * </ul>
     *
     * @throws IllegalArgumentException if the path is rejected
     */
    public void check(StoragePath path) {
        if ("file".equalsIgnoreCase(path.scheme()) == false) {
            return;
        }
        if (enabled == false) {
            throw new IllegalArgumentException(LOCAL_DISK_DISABLED_MESSAGE);
        }
        // For a glob location (e.g. file:///dir/*.parquet) the raw path is not a valid filesystem path —
        // resolving it would throw InvalidPathException on filesystems that reject glob metacharacters
        // (notably the Windows mock FS used in tests). Mirror LocalStorageProvider.toFilePath and validate the
        // longest non-glob prefix directory instead; patternPrefix() preserves any ".." so escapes are still caught.
        String localPath = path.isPattern() ? path.patternPrefix().localPath() : path.localPath();
        Path resolved = PathUtils.get(allowedRoots, localPath);
        if (resolved == null) {
            throw new IllegalArgumentException(PATH_OUTSIDE_ALLOWLIST_PREFIX + path);
        }
    }

    /**
     * Convenience overload that parses a raw location string and delegates to {@link #check(StoragePath)}.
     * {@code StoragePath} extracts the scheme, so any non-{@code file} scheme (and any {@code file} URI form,
     * including authority-less {@code file:/path}) is handled uniformly without relying on a literal prefix match.
     *
     * @throws IllegalArgumentException if the location is a {@code file://} path that is rejected
     */
    public void check(String location) {
        if (location != null) {
            check(StoragePath.of(location));
        }
    }
}
