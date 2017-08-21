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

package org.elasticsearch.gradle

import groovy.transform.Sortable
import java.util.regex.Matcher
import org.gradle.api.InvalidUserDataException

/**
 * Encapsulates comparison and printing logic for an x.y.z version.
 */
@Sortable(includes=['id'])
public class Version {

    final int major
    final int minor
    final int bugfix
    final int id
    final boolean snapshot
    /**
     * Suffix on the version name. Unlike Version.java the build does not
     * consider alphas and betas different versions, it just preserves the
     * suffix that the version was declared with in Version.java.
     */
    final String suffix

    public Version(int major, int minor, int bugfix,
            String suffix, boolean snapshot) {
        this.major = major
        this.minor = minor
        this.bugfix = bugfix
        this.snapshot = snapshot
        this.suffix = suffix
        this.id = major * 100000 + minor * 1000 + bugfix * 10 +
            (snapshot ? 1 : 0)
    }

    public static Version fromString(String s) {
        Matcher m = s =~ /(\d+)\.(\d+)\.(\d+)(-alpha\d+|-beta\d+|-rc\d+)?(-SNAPSHOT)?/
        if (m.matches() == false) {
            throw new InvalidUserDataException("Invalid version [${s}]")
        }
        return new Version(m.group(1) as int, m.group(2) as int,
            m.group(3) as int, m.group(4) ?: '', m.group(5) != null)
    }

    @Override
    public String toString() {
        String snapshotStr = snapshot ? '-SNAPSHOT' : ''
        return "${major}.${minor}.${bugfix}${suffix}${snapshotStr}"
    }

    public boolean before(String compareTo) {
        return id < fromString(compareTo).id
    }

    public boolean onOrBefore(String compareTo) {
        return id <= fromString(compareTo).id
    }

    public boolean onOrAfter(String compareTo) {
        return id >= fromString(compareTo).id
    }

    public boolean after(String compareTo) {
        return id > fromString(compareTo).id
    }
}
