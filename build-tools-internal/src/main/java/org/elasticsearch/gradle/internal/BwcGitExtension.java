/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.Version;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;

import javax.inject.Inject;
import java.io.File;

public class BwcGitExtension {

    private Provider<Version> bwcVersion;
    private Provider<String> bwcBranch;
    private Property<File> checkoutDir;

    @Inject
    public BwcGitExtension(ObjectFactory objectFactory) {
        this.checkoutDir = objectFactory.property(File.class);
    }

    public Provider<Version> getBwcVersion() {
        return bwcVersion;
    }

    public void setBwcVersion(Provider<Version> bwcVersion) {
        this.bwcVersion = bwcVersion;
    }

    public Provider<String> getBwcBranch() {
        return bwcBranch;
    }

    public void setBwcBranch(Provider<String> bwcBranch) {
        this.bwcBranch = bwcBranch;
    }

    public Property<File> getCheckoutDir() {
        return checkoutDir;
    }
}
