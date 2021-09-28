/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.support.Validation.Users;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.FileReloadListener;
import org.elasticsearch.xpack.security.support.SecurityFiles;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class FileUserPasswdStore {
    private static final Logger logger = LogManager.getLogger(FileUserPasswdStore.class);

    private final Path file;
    private final Settings settings;
    private final CopyOnWriteArrayList<Runnable> listeners;
    private volatile Map<String, char[]> users;

    public FileUserPasswdStore(RealmConfig config, ResourceWatcherService watcherService) {
        this(config, watcherService, () -> {});
    }

    FileUserPasswdStore(RealmConfig config, ResourceWatcherService watcherService, Runnable listener) {
        file = resolveFile(config.env());
        settings = config.settings();
        users = parseFileLenient(file, logger, settings);
        listeners = new CopyOnWriteArrayList<>(Collections.singletonList(listener));
        FileWatcher watcher = new FileWatcher(file.getParent());
        watcher.addListener(new FileReloadListener(file, this::tryReload));
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching users file [{}]", e, file.toAbsolutePath());
        }
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    public int usersCount() {
        return users.size();
    }

    public AuthenticationResult verifyPassword(String username, SecureString password, java.util.function.Supplier<User> user) {
        final char[] hash = users.get(username);
        if (hash == null) {
            return AuthenticationResult.notHandled();
        }
        if (Hasher.verifyHash(password, hash) == false) {
            return AuthenticationResult.unsuccessful("Password authentication failed for " + username, null);
        }
        return AuthenticationResult.success(user.get());
    }

    public boolean userExists(String username) {
        return users.containsKey(username);
    }

    public static Path resolveFile(Environment env) {
        return XPackPlugin.resolveConfigFile(env, "users");
    }

    /**
     * Internally in this class, we try to load the file, but if for some reason we can't, we're being more lenient by
     * logging the error and skipping all users. This is aligned with how we handle other auto-loaded files in security.
     */
    static Map<String, char[]> parseFileLenient(Path path, Logger logger, Settings settings) {
        try {
            Map<String, char[]> map = parseFile(path, logger, settings);
            return map == null ? emptyMap() : map;
        } catch (Exception e) {
            logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to parse users file [{}]. skipping/removing all users...", path.toAbsolutePath()), e);
            return emptyMap();
        }
    }

    /**
     * Parses the users file.
     *
     * Returns {@code null}, if the {@code users} file does not exist.
     */
    public static Map<String, char[]> parseFile(Path path, @Nullable Logger logger, Settings settings) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }
        logger.trace("reading users file [{}]...", path.toAbsolutePath());

        if (Files.exists(path) == false) {
            return null;
        }

        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new IllegalStateException("could not read users file [" + path.toAbsolutePath() + "]", ioe);
        }

        Map<String, char[]> users = new HashMap<>();

        final boolean allowReserved = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings) == false;
        int lineNr = 0;
        for (String line : lines) {
            lineNr++;
            if (line.startsWith("#")) { // comment
                continue;
            }

            // only trim the line because we have a format, our tool generates the formatted text and we shouldn't be lenient
            // and allow spaces in the format
            line = line.trim();
            int i = line.indexOf(':');
            if (i <= 0 || i == line.length() - 1) {
                logger.error("invalid entry in users file [{}], line [{}]. skipping...", path.toAbsolutePath(), lineNr);
                continue;
            }
            String username = line.substring(0, i);
            Validation.Error validationError = Users.validateUsername(username, allowReserved, settings);
            if (validationError != null) {
                logger.error("invalid username [{}] in users file [{}], skipping... ({})", username, path.toAbsolutePath(),
                        validationError);
                continue;
            }
            String hash = line.substring(i + 1);
            users.put(username, hash.toCharArray());
        }

        logger.debug("parsed [{}] users from file [{}]", users.size(), path.toAbsolutePath());
        return unmodifiableMap(users);
    }

    public static void writeFile(Map<String, char[]> users, Path path) {
        SecurityFiles.writeFileAtomically(
                path,
                users,
                e -> String.format(Locale.ROOT, "%s:%s", e.getKey(), new String(e.getValue())));
    }

    void notifyRefresh() {
        listeners.forEach(Runnable::run);
    }

    private void tryReload() {
        final Map<String, char[]> previousUsers = users;
        users = parseFileLenient(file, logger, settings);

        if (Maps.deepEquals(previousUsers, users) == false) {
            logger.info("users file [{}] changed. updating users...", file.toAbsolutePath());
            notifyRefresh();
        }
    }
}
