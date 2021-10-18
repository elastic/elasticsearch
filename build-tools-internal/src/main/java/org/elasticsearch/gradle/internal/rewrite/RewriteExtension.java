/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class RewriteExtension {
    private final List<String> activeRecipes = new ArrayList<>();
    private File configFile;
    private String rewriteVersion = "7.10.0";

    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }

    public File getConfigFile() {
        return configFile;
    }

    public void activeRecipe(String... recipes) {
        activeRecipes.addAll(asList(recipes));
    }

    public void setActiveRecipes(List<String> activeRecipes) {
        this.activeRecipes.clear();
        this.activeRecipes.addAll(activeRecipes);
    }

    public List<String> getActiveRecipes() {
        return activeRecipes;
    }

    public String getRewriteVersion() {
        return rewriteVersion;
    }

    public void setRewriteVersion(String value) {
        rewriteVersion = value;
    }

}
