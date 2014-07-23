/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class FileRolesStore extends AbstractComponent implements RolesStore {

    private static final Pattern COMMA_DELIM = Pattern.compile("\\s*,\\s*");

    private final Path file;
    private final FileWatcher watcher;

    private volatile ImmutableMap<String, Permission> roles;

    @Inject
    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService) {
        super(settings);
        file = resolveFile(componentSettings, env);
        roles = ImmutableMap.copyOf(parseFile(file, logger));
        watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher);
    }

    @Override
    public Permission permission(String... roles) {
        Permission.Compound.Builder builder = Permission.compound();
        for (int i = 0; i < roles.length; i++) {
            Permission permissions = this.roles.get(roles[i]);
            if (permissions != null) {
                builder.add(permissions);
            }
        }
        return builder.build();
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get("file.roles");
        if (location == null) {
            return env.configFile().toPath().resolve(".roles.yml");
        }
        return Paths.get(location);
    }

    public static Map<String, Permission> parseFile(Path path, @Nullable ESLogger logger) {
        if (!Files.exists(path)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, Permission> roles = ImmutableMap.builder();
        try {
            byte[] content = Streams.copyToByteArray(path.toFile());
            XContentParser parser = XContentFactory.xContent(content).createParser(content);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    String roleName = currentFieldName;
                    Permission.Compound.Builder builder = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if ("cluster".equals(currentFieldName)) {
                            String[] privs;
                            if (token == XContentParser.Token.VALUE_STRING) {
                                privs = COMMA_DELIM.split(parser.text().trim());
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                List<String> list = null;
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        if (list == null) {
                                            list = new ArrayList<>();
                                        }
                                        list.add(parser.text());
                                    }
                                }
                                privs = list != null ? list.toArray(new String[list.size()]) : Strings.EMPTY_ARRAY;
                            } else {
                                throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                        "]. [cluster] field value can either be a string or a list of strings, but [" + token + "] was found instead");
                            }
                            if (builder == null) {
                                builder = Permission.compound();
                            }
                            Privilege.Cluster cluster = Privilege.Cluster.resolve(privs);
                            builder.add(Permission.cluster(cluster));
                        } else if ("indices".equals(currentFieldName)) {
                            if (token != XContentParser.Token.START_ARRAY) {

                            }
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    Permission.Index index = parseIndicesPermission(parser.text());
                                    if (index != null) {
                                        if (builder == null) {
                                            builder = Permission.compound();
                                        }
                                        builder.add(index);
                                    }
                                } else {
                                    throw new ElasticsearchException("Invalid roles file format [" + path.toAbsolutePath() +
                                            "]. [indices] field value must be an array of indices-privileges mappings defined as a string" +
                                            " in the form <comma-separated list of index name patterns>::<comma-separated list of privileges> , but [" + token + "] was found instead");
                                }
                            }
                        }
                    }
                    assert roleName != null;
                    if (builder != null) {
                        roles.put(roleName, builder.build());
                    }
                }
            }

            return roles.build();

        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to read roles file [" + path.toAbsolutePath() + "]", ioe);
        }
    }

    private static Permission.Index parseIndicesPermission(String spec) {
        int i = spec.indexOf("::");
        if (i == 0) {
            throw new ElasticsearchException("Malformed index privileges entry [" + spec + "]. Missing indices name patterns list");
        }
        if (i < 0) {
            throw new ElasticsearchException("Malformed index privileges entry [" + spec + "]");
        }
        if (i == spec.length() - 2) {
            throw new ElasticsearchException("Malformed index privileges entry [" + spec + "]. Missing privileges list");
        }
        if (spec.indexOf("::", i+2) >= 0) {
            throw new ElasticsearchException("Malformed index privileges entry [" + spec + "]. There should only be a single \"::\" separator");
        }

        String[] indices = COMMA_DELIM.split(spec.substring(0, i));

        String[] privs = COMMA_DELIM.split(spec.substring(i+2));
        Privilege.Index index = Privilege.Index.resolve(privs);
        return Permission.index(index, indices);
    }

    private class FileListener extends FileChangesListener {
        @Override
        public void onFileCreated(File file) {
            if (file.equals(FileRolesStore.this.file.toFile())) {
                roles = ImmutableMap.copyOf(parseFile(file.toPath(), logger));
            }
        }

        @Override
        public void onFileDeleted(File file) {
            if (file.equals(FileRolesStore.this.file.toFile())) {
                roles = ImmutableMap.of();
            }
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(FileRolesStore.this.file.toFile())) {
                if (file.equals(FileRolesStore.this.file.toFile())) {
                    roles = ImmutableMap.copyOf(parseFile(file.toPath(), logger));
                }
            }
        }
    }
}
