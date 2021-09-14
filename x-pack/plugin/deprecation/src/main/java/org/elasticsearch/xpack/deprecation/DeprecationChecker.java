/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import java.util.List;

public interface DeprecationChecker {

    /**
     * Should this deprecation checker be enabled?
     *
     * @param settings Cluster settings
     * @return True if enabled given the settings
     */
    boolean enabled(Settings settings);

    /**
     * This runs the checks for the current deprecation checker.
     *
     * @param components The components provided for the checker
     * @param deprecationIssueListener The issues found
     */
    void check(Components components, ActionListener<CheckResult> deprecationIssueListener);

    /**
     * @return The name of the checker
     */
    String getName();

    class CheckResult {
        private final String checkerName;
        private final List<DeprecationIssue> issues;

        public CheckResult(String checkerName, List<DeprecationIssue> issues) {
            this.checkerName = checkerName;
            this.issues = issues;
        }

        public String getCheckerName() {
            return checkerName;
        }

        public List<DeprecationIssue> getIssues() {
            return issues;
        }
    }

    class Components {

        private final NamedXContentRegistry xContentRegistry;
        private final Settings settings;
        private final Client client;

        Components(NamedXContentRegistry xContentRegistry, Settings settings, OriginSettingClient client) {
            this.xContentRegistry = xContentRegistry;
            this.settings = settings;
            this.client = client;
        }

        public NamedXContentRegistry xContentRegistry() {
            return xContentRegistry;
        }

        public Settings settings() {
            return settings;
        }

        public Client client() {
            return client;
        }

    }
}
