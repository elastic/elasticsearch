/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.gradle.api.Project;

/**
 * A container for plugin properties that will be written to the plugin descriptor, for easy
 * manipulation in the gradle DSL.
 */
public class PluginPropertiesExtension extends BasePluginPropertiesExtension {

    private String classname;

    public PluginPropertiesExtension(Project project) {
        super(project);
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }
}
