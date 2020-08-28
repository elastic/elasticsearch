/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the logic of whether a new index should be automatically created when
 * a write operation is about to happen in a non existing index.
 */
public final class AutoCreateIndex {

    public static final Setting<AutoCreate> AUTO_CREATE_INDEX_SETTING =
        new Setting<>("action.auto_create_index", "true", AutoCreate::new, Property.NodeScope, Setting.Property.Dynamic);

    private final IndexNameExpressionResolver resolver;
    private volatile AutoCreate autoCreate;

    public AutoCreateIndex(Settings settings, ClusterSettings clusterSettings, IndexNameExpressionResolver resolver) {
        this.resolver = resolver;
        this.autoCreate = AUTO_CREATE_INDEX_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AUTO_CREATE_INDEX_SETTING, this::setAutoCreate);
    }

    /**
     * Do we really need to check if an index should be auto created?
     */
    public boolean needToCheck() {
        return this.autoCreate.autoCreateIndex;
    }

    /**
     * Should the index be auto created?
     * @throws IndexNotFoundException if the index doesn't exist and shouldn't be auto created
     */
    public boolean shouldAutoCreate(String index, ClusterState state) {
        if (resolver.hasIndexAbstraction(index, state)) {
            return false;
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
                throw new IndexNotFoundException("[" + AUTO_CREATE_INDEX_SETTING.getKey() + "] contains [-"
                        + indexExpression + "] which forbids automatic creation of the index", index);
            }
        }
        throw new IndexNotFoundException("[" + AUTO_CREATE_INDEX_SETTING.getKey() + "] ([" + autoCreate
                + "]) doesn't match", index);
    }

    AutoCreate getAutoCreate() {
        return autoCreate;
    }

    void setAutoCreate(AutoCreate autoCreate) {
        this.autoCreate = autoCreate;
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
                            throw new IllegalArgumentException("Can't parse [" + value + "] for setting [action.auto_create_index] must "
                                    + "be either [true, false, or a comma separated list of index patterns]");
                        }
                        pattern = pattern.trim();
                        Tuple<String, Boolean> expression;
                        if (pattern.startsWith("-")) {
                            if (pattern.length() == 1) {
                                throw new IllegalArgumentException("Can't parse [" + value + "] for setting [action.auto_create_index] "
                                        + "must contain an index name after [-]");
                            }
                            expression = new Tuple<>(pattern.substring(1), false);
                        } else if(pattern.startsWith("+")) {
                            if (pattern.length() == 1) {
                                throw new IllegalArgumentException("Can't parse [" + value + "] for setting [action.auto_create_index] "
                                        + "must contain an index name after [+]");
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
