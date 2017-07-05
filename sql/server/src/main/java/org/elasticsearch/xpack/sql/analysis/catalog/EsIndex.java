/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import static java.util.Collections.emptyList;

public class EsIndex {

    public static final EsIndex NOT_FOUND = new EsIndex("", emptyList(), emptyList(), Settings.EMPTY);

    private final String name;
    private final List<String> types;
    private final List<String> aliases;
    private final Settings settings;

    public EsIndex(String name, List<String> types, List<String> aliases, Settings settings) {
        this.name = name;
        this.types = types;
        this.aliases = aliases;
        this.settings = settings;
    }

    public String name() {
        return name;
    }


    public List<String> types() {
        return types;
    }


    public List<String> aliases() {
        return aliases;
    }


    public Settings settings() {
        return settings;
    }

    static List<EsIndex> build(Iterator<IndexMetaData> metadata) {
        if (metadata == null || !metadata.hasNext()) {
            return emptyList();
        }
        List<EsIndex> list = new ArrayList<>();
        while (metadata.hasNext()) {
            list.add(build(metadata.next()));
        }
        return list;
    }

    static EsIndex build(IndexMetaData metadata) {
        List<String> types = Arrays.asList(metadata.getMappings().keys().toArray(String.class));
        List<String> aliases = Arrays.asList(metadata.getAliases().keys().toArray(String.class));
        return new EsIndex(metadata.getIndex().getName(), types, aliases, metadata.getSettings());
    }
}
