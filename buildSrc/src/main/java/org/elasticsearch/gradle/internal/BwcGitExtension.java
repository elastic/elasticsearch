/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
