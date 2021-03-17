/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.elasticsearch.xpack.security.support.FileLineParser;
import org.elasticsearch.xpack.security.support.FileReloadListener;
import org.elasticsearch.xpack.security.support.SecurityFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class FileServiceAccountsTokenStore implements ServiceAccountsTokenStore {

    private static final Logger logger = LogManager.getLogger(FileServiceAccountsTokenStore.class);

    private final Path file;
    private final CopyOnWriteArrayList<Runnable> listeners;
    private volatile Map<String, char[]> tokenHashes;

    public FileServiceAccountsTokenStore(Environment env, ResourceWatcherService resourceWatcherService) {
        file = resolveFile(env);
        FileWatcher watcher = new FileWatcher(file.getParent());
        watcher.addListener(new FileReloadListener(file, this::tryReload));
        try {
            resourceWatcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching service_tokens file [{}]", e, file.toAbsolutePath());
        }
        try {
            tokenHashes = parseFile(file, logger);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load service_tokens file [" + file + "]", e);
        }
        listeners = new CopyOnWriteArrayList<>();
    }

    @Override
    public boolean authenticate(ServiceAccountToken token) {
        return false;
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    private void notifyRefresh() {
        listeners.forEach(Runnable::run);
    }

    private void tryReload() {
        final Map<String, char[]> previousTokenHashes = tokenHashes;
        tokenHashes = parseFileLenient(file, logger);
        if (false == Maps.deepEquals(tokenHashes, previousTokenHashes)) {
            logger.info("service tokens file [{}] changed. updating ...", file.toAbsolutePath());
            notifyRefresh();
        }
    }

    // package private for testing
    Map<String, char[]> getTokenHashes() {
        return tokenHashes;
    }

    static Path resolveFile(Environment env) {
        return XPackPlugin.resolveConfigFile(env, "service_tokens");
    }

    static Map<String, char[]> parseFileLenient(Path path, @Nullable Logger logger) {
        try {
            return parseFile(path, logger);
        } catch (Exception e) {
            logger.error("failed to parse service tokens file [{}]. skipping/removing all tokens...",
                path.toAbsolutePath());
            return Map.of();
        }
    }

    static Map<String, char[]> parseFile(Path path, @Nullable Logger logger) throws IOException {
        final Logger thisLogger = logger == null ? NoOpLogger.INSTANCE : logger;
        thisLogger.trace("reading service_tokens file [{}]...", path.toAbsolutePath());
        if (Files.exists(path) == false) {
            thisLogger.trace("file [{}] does not exist", path.toAbsolutePath());
            return Map.of();
        }
        final Map<String, char[]> parsedTokenHashes = new HashMap<>();
        FileLineParser.parse(path, (lineNumber, line) -> {
            line = line.trim();
            final int colon = line.indexOf(':');
            if (colon == -1) {
                thisLogger.warn("invalid format at line #{} of service_tokens file [{}] - missing ':' character - ", lineNumber, path);
                throw new IllegalStateException("Missing ':' character at line #" + lineNumber);
            }
            final String key = line.substring(0, colon);
            // TODO: validate against known service accounts?
            char[] hash = new char[line.length() - (colon + 1)];
            line.getChars(colon + 1, line.length(), hash, 0);
            if (Hasher.resolveFromHash(hash) == Hasher.NOOP) {
                thisLogger.warn("skipping plaintext service account token for key [{}]", key);
            } else {
                thisLogger.trace("parsed tokens for key [{}]", key);
                final char[] previousHash = parsedTokenHashes.put(key, hash);
                if (previousHash != null) {
                    thisLogger.warn("found duplicated key [{}], earlier entries are overridden", key);
                }
            }
        });
        thisLogger.debug("parsed [{}] tokens from file [{}]", parsedTokenHashes.size(), path.toAbsolutePath());
        return Map.copyOf(parsedTokenHashes);
    }

    static void writeFile(Path path, Map<String, char[]> tokenHashes) {
        SecurityFiles.writeFileAtomically(
            path, tokenHashes, e -> String.format(Locale.ROOT, "%s:%s", e.getKey(), new String(e.getValue())));
    }
}
