/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the logic of whether a new index should be automatically created when
 * a write operation is about to happen in a non existing index.
 */
public final class AutoCreateIndex {
    public static final Setting<AutoCreate> AUTO_CREATE_INDEX_SETTING = new Setting<>(
        "action.auto_create_index",
        "true",
        AutoCreate::new,
        Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final IndexNameExpressionResolver resolver;
    private final SystemIndices systemIndices;
    private volatile AutoCreate autoCreate;

    public AutoCreateIndex(
        Settings settings,
        ClusterSettings clusterSettings,
        IndexNameExpressionResolver resolver,
        SystemIndices systemIndices
    ) {
        this.resolver = resolver;
        this.systemIndices = systemIndices;
        this.autoCreate = AUTO_CREATE_INDEX_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AUTO_CREATE_INDEX_SETTING, this::setAutoCreate);
    }

    /**
     * Should the index be auto created?
     * @throws IndexNotFoundException if the index doesn't exist and shouldn't be auto created
     */
    public boolean shouldAutoCreate(String index, ClusterState state) {
        if (resolver.hasIndexAbstraction(index, state)) {
            return false;
        }

        // Always auto-create system indexes
        if (systemIndices.isSystemName(index)) {
            return true;
        }

        // Templates can override the AUTO_CREATE_INDEX_SETTING setting
        final ComposableIndexTemplate template = findTemplate(index, state.metadata());
        if (template != null && template.getAllowAutoCreate() != null) {
            if (template.getAllowAutoCreate()) {
                return true;
            } else {
                // An explicit false value overrides AUTO_CREATE_INDEX_SETTING
                throw new IndexNotFoundException("composable template " + template.indexPatterns() + " forbids index auto creation");
            }
        }

        // One volatile read, so that all checks are done against the same instance:
        final AutoCreate autoCreate = this.autoCreate;
        if (autoCreate.autoCreateIndex == false) {
            throw new IndexNotFoundException("[" + AUTO_CREATE_INDEX_SETTING.getKey() + "] is [false]", index);
        }

        // matches not set, default value of "true"
        if (autoCreate.expressions.isEmpty()) {
            return true;
        }
        for (Tuple<String, Boolean> expression : autoCreate.expressions) {
            String indexExpression = expression.v1();
            boolean include = expression.v2();
            if (Regex.simpleMatch(indexExpression, index)) {
                if (include) {
                    return true;
                }
                throw new IndexNotFoundException(
                    "["
                        + AUTO_CREATE_INDEX_SETTING.getKey()
                        + "] contains [-"
                        + indexExpression
                        + "] which forbids automatic creation of the index",
                    index
                );
            }
        }
        throw new IndexNotFoundException("[" + AUTO_CREATE_INDEX_SETTING.getKey() + "] ([" + autoCreate + "]) doesn't match", index);
    }

    AutoCreate getAutoCreate() {
        return autoCreate;
    }

    void setAutoCreate(AutoCreate autoCreate) {
        this.autoCreate = autoCreate;
    }

    private static ComposableIndexTemplate findTemplate(String indexName, Metadata metadata) {
        final String templateName = MetadataIndexTemplateService.findV2Template(metadata, indexName, false);
        return metadata.templatesV2().get(templateName);
    }

    static class AutoCreate {
        private final boolean autoCreateIndex;
        private final List<Tuple<String, Boolean>> expressions;
        private final String string;

        private AutoCreate(String value) {
            boolean autoCreateIndex;
            List<Tuple<String, Boolean>> expressions = new ArrayList<>();
            try {
                autoCreateIndex = Booleans.parseBoolean(value);
            } catch (IllegalArgumentException ex) {
                try {
                    String[] patterns = Strings.commaDelimitedListToStringArray(value);
                    for (String pattern : patterns) {
                        if (pattern == null || pattern.trim().length() == 0) {
                            throw new IllegalArgumentException(
                                "Can't parse ["
                                    + value
                                    + "] for setting [action.auto_create_index] must "
                                    + "be either [true, false, or a comma separated list of index patterns]"
                            );
                        }
                        pattern = pattern.trim();
                        Tuple<String, Boolean> expression;
                        if (pattern.startsWith("-")) {
                            if (pattern.length() == 1) {
                                throw new IllegalArgumentException(
                                    "Can't parse ["
                                        + value
                                        + "] for setting [action.auto_create_index] "
                                        + "must contain an index name after [-]"
                                );
                            }
                            expression = new Tuple<>(pattern.substring(1), false);
                        } else if (pattern.startsWith("+")) {
                            if (pattern.length() == 1) {
                                throw new IllegalArgumentException(
                                    "Can't parse ["
                                        + value
                                        + "] for setting [action.auto_create_index] "
                                        + "must contain an index name after [+]"
                                );
                            }
                            expression = new Tuple<>(pattern.substring(1), true);
                        } else {
                            expression = new Tuple<>(pattern, true);
                        }
                        expressions.add(expression);
                    }
                    autoCreateIndex = true;
                } catch (IllegalArgumentException ex1) {
                    ex1.addSuppressed(ex);
                    throw ex1;
                }
            }
            this.expressions = expressions;
            this.autoCreateIndex = autoCreateIndex;
            this.string = value;
        }

        boolean isAutoCreateIndex() {
            return autoCreateIndex;
        }

        List<Tuple<String, Boolean>> getExpressions() {
            return expressions;
        }

        @Override
        public String toString() {
            return string;
        }
    }
}
