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
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;

import java.io.File;

public class BwcGitExtension {

    private Property<Version> bwcVersion;
    private Property<String> bwcBranch;
    private RegularFileProperty checkoutDir;

    BwcGitExtension(ObjectFactory objectFactory) {
        bwcVersion = objectFactory.property(Version.class);
        bwcBranch = objectFactory.property(String.class);
        checkoutDir = objectFactory.fileProperty();
    }

    public Property<Version> getBwcVersion() {
        return bwcVersion;
    }

    public void setBwcVersion(Property<Version> bwcVersion) {
        this.bwcVersion = bwcVersion;
    }

    public Property<String> getBwcBranch() {
        return bwcBranch;
    }

    public void setBwcBranch(Property<String> bwcBranch) {
        this.bwcBranch = bwcBranch;
    }

    public RegularFileProperty getCheckoutDir() {
        return checkoutDir;
    }

    public void setCheckoutDir(File checkoutDir) {
        this.checkoutDir.set(checkoutDir);
    }
}
