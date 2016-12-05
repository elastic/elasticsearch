/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.file;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.support.NoOpLogger;
import org.elasticsearch.xpack.security.support.Validation;
import org.elasticsearch.xpack.security.support.Validation.Users;

import java.io.IOException;
import java.io.PrintWriter;
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
import static org.elasticsearch.xpack.security.support.SecurityFiles.openAtomicMoveWriter;

public class FileUserPasswdStore {

    private final Logger logger;

    private final Path file;
    private final Hasher hasher = Hasher.BCRYPT;
    private final Settings settings;
    private final CopyOnWriteArrayList<Runnable> listeners;

    private volatile Map<String, char[]> users;

    public FileUserPasswdStore(RealmConfig config, ResourceWatcherService watcherService) {
        this(config, watcherService, () -> {});
    }

    FileUserPasswdStore(RealmConfig config, ResourceWatcherService watcherService, Runnable listener) {
        logger = config.logger(FileUserPasswdStore.class);
        file = resolveFile(config.env());
        settings = config.globalSettings();
        users = parseFileLenient(file, logger, settings);
        listeners = new CopyOnWriteArrayList<>(Collections.singletonList(listener));
        FileWatcher watcher = new FileWatcher(file.getParent());
        watcher.addListener(new FileListener());
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

    public boolean verifyPassword(String username, SecuredString password) {
        char[] hash = users.get(username);
        return hash != null && hasher.verify(password, hash);
    }

    public boolean userExists(String username) {
        return users != null && users.containsKey(username);
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
            return parseFile(path, logger, settings);
        } catch (Exception e) {
            logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to parse users file [{}]. skipping/removing all users...", path.toAbsolutePath()), e);
            return emptyMap();
        }
    }

    /**
     * parses the users file. Should never return {@code null}, if the file doesn't exist an
     * empty map is returned
     */
    public static Map<String, char[]> parseFile(Path path, @Nullable Logger logger, Settings settings) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }
        logger.trace("reading users file [{}]...", path.toAbsolutePath());

        if (!Files.exists(path)) {
            return emptyMap();
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
            int i = line.indexOf(":");
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
        try (PrintWriter writer = new PrintWriter(openAtomicMoveWriter(path))) {
            for (Map.Entry<String, char[]> entry : users.entrySet()) {
                writer.printf(Locale.ROOT, "%s:%s%s", entry.getKey(), new String(entry.getValue()), System.lineSeparator());
            }
        } catch (IOException ioe) {
            throw new ElasticsearchException("could not write file [{}], please check file permissions", ioe, path.toAbsolutePath());
        }
    }

    void notifyRefresh() {
        listeners.forEach(Runnable::run);
    }

    private class FileListener implements FileChangesListener {
        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            if (file.equals(FileUserPasswdStore.this.file)) {
                logger.info("users file [{}] changed. updating users... )", file.toAbsolutePath());
                users = parseFileLenient(file, logger, settings);
                notifyRefresh();
            }
        }
    }
}
