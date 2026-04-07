/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;

import java.util.Map;

public abstract class AbstractDependenciesTask extends DefaultTask {

    @Input
    @Optional
    public abstract MapProperty<String, String> getMappings();

    /**
     * Add a mapping from a regex pattern for the jar name, to a prefix to find
     * the LICENSE and NOTICE file for that jar.
     */
    public void mapping(Map<String, String> props) {
        String from = props.get("from");
        if (from == null) {
            throw new InvalidUserDataException("Missing \"from\" setting for license name mapping");
        }
        String to = props.get("to");
        if (to == null) {
            throw new InvalidUserDataException("Missing \"to\" setting for license name mapping");
        }
        if (props.size() > 2) {
            throw new InvalidUserDataException("Unknown properties for mapping on dependencyLicenses: " + props.keySet());
        }
        getMappings().put(from, to);
    }
}
