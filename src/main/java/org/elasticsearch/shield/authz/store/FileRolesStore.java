/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.scanner.ScannerException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *
 */
public class FileRolesStore extends AbstractComponent implements RolesStore {

    private static final Pattern COMMA_DELIM = Pattern.compile("\\s*,\\s*");

    private final Path file;
    private final Listener listener;

    private volatile ImmutableMap<String, Permission.Global> permissions;

    @Inject
    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        file = resolveFile(componentSettings, env);
        permissions = parseFile(file);
        FileWatcher watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        this.listener = listener;
    }

    @Override
    public Permission.Global permission(String role) {
        return permissions.get(role);
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get("files.roles");
        if (location == null) {
            return env.configFile().toPath().resolve(".roles.yml");
        }
        return Paths.get(location);
    }

    public static ImmutableMap<String, Permission.Global> parseFile(Path path) {
        if (!Files.exists(path)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, Permission.Global> roles = ImmutableMap.builder();
        try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ)) {
            XContentParser parser = YamlXContent.yamlXContent.createParser(input);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT && token != null) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    String roleName = currentFieldName;
                    Permission.Global.Builder permission = Permission.Global.builder();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if ("cluster".equals(currentFieldName)) {
                            Privilege.Name name;
                            if (token == XContentParser.Token.VALUE_STRING) {
                                String[] names = COMMA_DELIM.split(parser.text().trim());
                                name = new Privilege.Name(names);
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                ImmutableSet.Builder<String> names = ImmutableSet.builder();
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        names.add(parser.text());
                                    }
                                }
                                name = new Privilege.Name(names.build());
                            } else {
                                throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                        "]. [cluster] field value can either be a string or a list of strings, but [" + token + "] was found instead");
                            }
                            permission.set(Privilege.Cluster.get(name));
                        } else if ("indices".equals(currentFieldName)) {
                            if (token == XContentParser.Token.START_OBJECT) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        currentFieldName = parser.currentName();
                                    } else {
                                        String[] indices = COMMA_DELIM.split(currentFieldName);
                                        Privilege.Name name;
                                        if (token == XContentParser.Token.VALUE_STRING) {
                                            String[] names = COMMA_DELIM.split(parser.text());
                                            name = new Privilege.Name(names);
                                        } else if (token == XContentParser.Token.START_ARRAY) {
                                            Set<String> names = new HashSet<>();
                                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                                if (token == XContentParser.Token.VALUE_STRING) {
                                                    names.add(parser.text());
                                                } else {
                                                    throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                                            "]. Could not parse [" + token + "] as index privilege. Privilege names must be strings");
                                                }
                                            }
                                            name = new Privilege.Name(names);
                                        } else {
                                            throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                                    "]. Could not parse [" + token + "] as index privileges list. Privilege lists must either " +
                                                    "be a comma delimited string or an array of strings");
                                        }
                                        permission.add(Privilege.Index.get(name), indices);
                                    }
                                }
                            } else {
                                throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                        "]. [indices] field value must be an array of indices-privileges mappings defined as a string" +
                                        " in the form <comma-separated list of index name patterns>::<comma-separated list of privileges> , but [" + token + "] was found instead");
                            }
                        } else {
                            throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                    "]. each role may have [cluster] field (holding a list of cluster permissions) and/or " +
                                    "[indices] field (holding a list of indices permissions. But [" + token + "] was found instead");
                        }
                    }
                    roles.put(roleName, permission.build());
                }
            }

            return roles.build();

        } catch (YAMLException|IOException ioe) {
            throw new ElasticsearchException("Failed to read roles file [" + path.toAbsolutePath() + "]", ioe);
        }
    }

    public static void writeFile(Map<String, Permission.Global> roles, Path path) {
        try (OutputStream output = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            XContentBuilder builder = XContentFactory.yamlBuilder(output);
            for (Map.Entry<String, Permission.Global> entry : roles.entrySet()) {
                builder.startObject(entry.getKey());
                Permission.Global permission = entry.getValue();
                Permission.Cluster cluster = permission.cluster();
                if (cluster != null && cluster.privilege() != Privilege.Cluster.NONE) {
                    builder.field("cluster", cluster.privilege().name());
                }
                Permission.Indices indices = permission.indices();
                if (indices != null) {
                    Permission.Global.Indices.Group[] groups = indices.groups();
                    if (groups != null && groups.length > 0) {
                        builder.startObject("indices");
                        for (int i = 0; i < groups.length; i++) {
                            builder.field(Strings.arrayToCommaDelimitedString(groups[i].indices())).value(groups[i].privilege().name());
                        }
                        builder.endObject();
                    }
                }
            }
        } catch (IOException ioe) {
            throw new ElasticsearchException("Could not write roles file [" + path.toAbsolutePath() + "], please check file permissions", ioe);
        }
    }

    public static interface Listener {

        public static final Listener NOOP = new Listener() {
            @Override
            public void onRefresh() {
            }
        };

        void onRefresh();
    }

    private class FileListener extends FileChangesListener {
        @Override
        public void onFileCreated(File file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(File file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(FileRolesStore.this.file.toFile())) {
                permissions = parseFile(file.toPath());
                listener.onRefresh();
            }
        }
    }
}
