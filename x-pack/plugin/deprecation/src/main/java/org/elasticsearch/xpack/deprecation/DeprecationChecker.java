/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;

public interface DeprecationChecker {

    interface Components {
        NamedXContentRegistry xContentRegistry();
        Settings settings();
        Client client();
    }

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
    void check(Components components, ActionListener<List<DeprecationIssue>> deprecationIssueListener);

    /**
     * @return The name of the checker
     */
    String getName();
}
