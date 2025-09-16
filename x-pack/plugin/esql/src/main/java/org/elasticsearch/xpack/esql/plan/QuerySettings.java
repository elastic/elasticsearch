/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.function.Predicate;

public enum QuerySettings {
    // TODO check cluster state and see if project routing is allowed
    // see https://github.com/elastic/elasticsearch/pull/134446
    // PROJECT_ROUTING(..., state -> state.getRemoteClusterNames().crossProjectEnabled());
    PROJECT_ROUTING(
        "project_routing",
        DataType.KEYWORD,
        true,
        false,
        true,
        "A project routing expression, "
            + "used to define which projects to route the query to. "
            + "Only supported if Cross-Project Search is enabled."
    ),;

    private String settingName;
    private DataType type;
    private final boolean serverlessOnly;
    private final boolean snapshotOnly;
    private final boolean preview;
    private final String description;
    private final Predicate<RemoteClusterService> validator;

    QuerySettings(
        String name,
        DataType type,
        boolean serverlessOnly,
        boolean preview,
        boolean snapshotOnly,
        String description,
        Predicate<RemoteClusterService> validator
    ) {
        this.settingName = name;
        this.type = type;
        this.serverlessOnly = serverlessOnly;
        this.preview = preview;
        this.snapshotOnly = snapshotOnly;
        this.description = description;
        this.validator = validator;
    }

    QuerySettings(String name, DataType type, boolean serverlessOnly, boolean preview, boolean snapshotOnly, String description) {
        this(name, type, serverlessOnly, preview, snapshotOnly, description, state -> true);
    }

    public String settingName() {
        return settingName;
    }

    public DataType type() {
        return type;
    }

    public boolean serverlessOnly() {
        return serverlessOnly;
    }

    public boolean snapshotOnly() {
        return snapshotOnly;
    }

    public boolean preview() {
        return preview;
    }

    public String description() {
        return description;
    }

    public Predicate<RemoteClusterService> validator() {
        return validator;
    }

    public static void validate(EsqlStatement statement, RemoteClusterService clusterService) {
        for (QuerySetting setting : statement.settings()) {
            boolean found = false;
            for (QuerySettings qs : values()) {
                if (qs.settingName().equals(setting.name())) {
                    found = true;
                    if (setting.value().dataType() != qs.type()) {
                        throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must be of type " + qs.type());
                    }
                    if (qs.validator().test(clusterService) == false) {
                        throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] is not allowed");
                    }
                    break;
                }
            }
            if (found == false) {
                throw new ParsingException(setting.source(), "Unknown setting [" + setting.name() + "]");
            }
        }
    }
}
