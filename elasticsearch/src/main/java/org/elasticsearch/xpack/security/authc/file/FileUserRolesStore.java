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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.support.NoOpLogger;
import org.elasticsearch.xpack.security.support.Validation;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.security.support.SecurityFiles.openAtomicMoveWriter;

public class FileUserRolesStore {

    private static final Pattern USERS_DELIM = Pattern.compile("\\s*,\\s*");

    private final Logger logger;

    private final Path file;
    private final CopyOnWriteArrayList<Runnable> listeners;
    private volatile Map<String, String[]> userRoles;

    FileUserRolesStore(RealmConfig config, ResourceWatcherService watcherService) {
        this(config, watcherService, () -> {});
    }

    FileUserRolesStore(RealmConfig config, ResourceWatcherService watcherService, Runnable listener) {
        logger = config.logger(FileUserRolesStore.class);
        file = resolveFile(config.env());
        userRoles = parseFileLenient(file, logger);
        listeners = new CopyOnWriteArrayList<>(Collections.singletonList(listener));
        FileWatcher watcher = new FileWatcher(file.getParent());
        watcher.addListener(new FileListener());
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching the user roles file [" + file.toAbsolutePath() + "]", e);
        }
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    int entriesCount() {
        return userRoles.size();
    }

    public String[] roles(String username) {
        if (userRoles == null) {
            return Strings.EMPTY_ARRAY;
        }
        String[] roles = userRoles.get(username);
        return roles == null ? Strings.EMPTY_ARRAY : userRoles.get(username);
    }

    public static Path resolveFile(Environment env) {
        return XPackPlugin.resolveConfigFile(env, "users_roles");
    }

    /**
     * Internally in this class, we try to load the file, but if for some reason we can't, we're being more lenient by
     * logging the error and skipping all enries. This is aligned with how we handle other auto-loaded files in security.
     */
    static Map<String, String[]> parseFileLenient(Path path, Logger logger) {
        try {
            return parseFile(path, logger);
        } catch (Exception e) {
            logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage("failed to parse users_roles file [{}]. skipping/removing all entries...",
                            path.toAbsolutePath()),
                    e);
            return emptyMap();
        }
    }

    /**
     * parses the users_roles file. Should never return return {@code null}, if the file doesn't exist
     * an empty map is returned. The read file holds a mapping per line of the form "role -&gt; users" while the returned
     * map holds entries of the form  "user -&gt; roles".
     */
    public static Map<String, String[]> parseFile(Path path, @Nullable Logger logger) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }
        logger.trace("reading users_roles file [{}]...", path.toAbsolutePath());


        if (!Files.exists(path)) {
            return emptyMap();
        }

        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new ElasticsearchException("could not read users file [" + path.toAbsolutePath() + "]", ioe);
        }

        Map<String, List<String>> userToRoles = new HashMap<>();

        int lineNr = 0;
        for (String line : lines) {
            lineNr++;
            if (line.startsWith("#")) {  //comment
                continue;
            }
            int i = line.indexOf(":");
            if (i <= 0 || i == line.length() - 1) {
                logger.error("invalid entry in users_roles file [{}], line [{}]. skipping...", path.toAbsolutePath(), lineNr);
                continue;
            }
            String role = line.substring(0, i).trim();
            Validation.Error validationError = Validation.Roles.validateRoleName(role, true);
            if (validationError != null) {
                logger.error("invalid role entry in users_roles file [{}], line [{}] - {}. skipping...", path.toAbsolutePath(), lineNr,
                        validationError);
                continue;
            }
            String usersStr = line.substring(i + 1).trim();
            if (Strings.isEmpty(usersStr)) {
                logger.error("invalid entry for role [{}] in users_roles file [{}], line [{}]. no users found. skipping...", role,
                        path.toAbsolutePath(), lineNr);
                continue;
            }
            String[] roleUsers = USERS_DELIM.split(usersStr);
            if (roleUsers.length == 0) {
                logger.error("invalid entry for role [{}] in users_roles file [{}], line [{}]. no users found. skipping...", role,
                        path.toAbsolutePath(), lineNr);
                continue;
            }

            for (String user : roleUsers) {
                List<String> roles = userToRoles.get(user);
                if (roles == null) {
                    roles = new ArrayList<>();
                    userToRoles.put(user, roles);
                }
                roles.add(role);
            }
        }

        Map<String, String[]> usersRoles = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : userToRoles.entrySet()) {
            usersRoles.put(entry.getKey(), entry.getValue().toArray(new String[entry.getValue().size()]));
        }

        logger.debug("parsed [{}] user to role mappings from file [{}]", usersRoles.size(), path.toAbsolutePath());
        return unmodifiableMap(usersRoles);
    }

    /**
     * Accepts a mapping of user -&gt; list of roles
     */
    public static void writeFile(Map<String, String[]> userToRoles, Path path) {
        HashMap<String, List<String>> roleToUsers = new HashMap<>();
        for (Map.Entry<String, String[]> entry : userToRoles.entrySet()) {
            for (String role : entry.getValue()) {
                List<String> users = roleToUsers.get(role);
                if (users == null) {
                    users = new ArrayList<>();
                    roleToUsers.put(role, users);
                }
                users.add(entry.getKey());
            }
        }

        try (PrintWriter writer = new PrintWriter(openAtomicMoveWriter(path))) {
            for (Map.Entry<String, List<String>> entry : roleToUsers.entrySet()) {
                writer.printf(Locale.ROOT, "%s:%s%s", entry.getKey(), Strings.collectionToCommaDelimitedString(entry.getValue()),
                        System.lineSeparator());
            }
        } catch (IOException ioe) {
            throw new ElasticsearchException("could not write file [" + path.toAbsolutePath() + "], please check file permissions", ioe);
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
            if (file.equals(FileUserRolesStore.this.file)) {
                logger.info("users roles file [{}] changed. updating users roles...", file.toAbsolutePath());
                userRoles = parseFileLenient(file, logger);
                notifyRefresh();
            }
        }
    }
}
