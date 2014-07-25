/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.support.UserRolesStore;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class FileUserRolesStore extends AbstractComponent implements UserRolesStore {

    private static final Pattern ROLES_DELIM = Pattern.compile("\\s*,\\s*");

    private final Path file;

    private volatile ImmutableMap<String, String[]> userRoles;

    private final Listener listener;

    @Inject
    public FileUserRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    FileUserRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        file = resolveFile(settings, env);
        userRoles = ImmutableMap.copyOf(parseFile(file, logger));
        FileWatcher watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher);
        this.listener = listener;
    }

    public String[] roles(String username) {
        return userRoles != null ? userRoles.get(username) : null;
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get("shield.authc.esusers.files.users_roles");
        if (location == null) {
            return env.configFile().toPath().resolve(".users_roles");
        }
        return Paths.get(location);
    }

    public static Map<String, String[]> parseFile(Path path, @Nullable ESLogger logger) {
        if (!Files.exists(path)) {
            return ImmutableMap.of();
        }

        List<String> lines = null;
        try {
            lines = Files.readAllLines(path, Charsets.UTF_8);
        } catch (IOException ioe) {
            throw new ElasticsearchException("Could not read users file [" + path.toAbsolutePath() + "]", ioe);
        }

        ImmutableMap.Builder<String, String[]> usersRoles = ImmutableMap.builder();

        int lineNr = 0;
        for (String line : lines) {
            lineNr++;
            int i = line.indexOf(":");
            if (i <= 0 || i == line.length() - 1) {
                logger.error("Invalid entry in users file [" + path.toAbsolutePath() + "], line [" + lineNr + "]. Skipping...");
                continue;
            }
            String username = line.substring(0, i).trim();
            String rolesStr = line.substring(i + 1).trim();
            String[] roles = ROLES_DELIM.split(rolesStr);
            usersRoles.put(username, roles);
        }

        return usersRoles.build();
    }

    public static void writeFile(Map<String, String[]> userRoles, Path path) {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(path, Charsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE))) {
            for (Map.Entry<String, String[]> entry : userRoles.entrySet()) {
                writer.printf(Locale.ROOT, "%s:%s%s", entry.getKey(), Strings.arrayToCommaDelimitedString(entry.getValue()), System.lineSeparator());
            }
        } catch (IOException ioe) {
            throw new ElasticsearchException("Could not write users file [" + path.toAbsolutePath() + "], please check file permissions");
        }
    }

    private class FileListener extends FileChangesListener {
        @Override
        public void onFileCreated(File file) {
            if (file.equals(FileUserRolesStore.this.file.toFile())) {
                userRoles = ImmutableMap.copyOf(parseFile(file.toPath(), logger));
                listener.onRefresh();
            }
        }

        @Override
        public void onFileDeleted(File file) {
            if (file.equals(FileUserRolesStore.this.file.toFile())) {
                userRoles = ImmutableMap.of();
                listener.onRefresh();
            }
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(FileUserRolesStore.this.file.toFile())) {
                if (file.equals(FileUserRolesStore.this.file.toFile())) {
                    userRoles = ImmutableMap.copyOf(parseFile(file.toPath(), logger));
                    listener.onRefresh();
                }
            }
        }
    }

    public static interface Listener {

        static final Listener NOOP = new Listener() {
            @Override
            public void onRefresh() {
            }
        };

        void onRefresh();
    }
}
