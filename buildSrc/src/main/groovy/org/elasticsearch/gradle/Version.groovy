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
    final int revision
    final int id
    final boolean snapshot
    /**
     * Suffix on the version name.
     */
    final String suffix

    public Version(int major, int minor, int revision,
            String suffix, boolean snapshot) {
        this.major = major
        this.minor = minor
        this.revision = revision
        this.snapshot = snapshot
        this.suffix = suffix

        int suffixOffset = 0
        if (suffix.contains("alpha")) {
          suffixOffset += Integer.parseInt(suffix.substring(6))
        } else if (suffix.contains("beta")) {
          suffixOffset += 25 + Integer.parseInt(suffix.substring(5))
        } else if (suffix.contains("rc")) {
          suffixOffset += 50 + Integer.parseInt(suffix.substring(3));
        }

        this.id = major * 1000000 + minor * 10000 + revision * 100 + suffixOffset
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
        return "${major}.${minor}.${revision}${suffix}${snapshotStr}"
    }

    public boolean before(Version compareTo) {
        return id < compareTo.id
    }

    public boolean before(String compareTo) {
        return before(fromString(compareTo))
    }

    public boolean onOrBefore(Version compareTo) {
        return id <= compareTo.id
    }

    public boolean onOrBefore(String compareTo) {
        return onOrBefore(fromString(compareTo))
    }

    public boolean onOrAfter(Version compareTo) {
        return id >= compareTo.id
    }

    public boolean onOrAfter(String compareTo) {
        return onOrAfter(fromString(compareTo))
    }

    public boolean after(Version compareTo) {
        return id > compareTo.id
    }

    public boolean after(String compareTo) {
        return after(fromString(compareTo))
    }

    public boolean onOrBeforeIncludingSuffix(Version otherVersion) {
        if (id != otherVersion.id) {
            return id < otherVersion.id
        }

        if (suffix == '') {
            return otherVersion.suffix == ''
        }

        return otherVersion.suffix == '' || suffix < otherVersion.suffix
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        Version version = (Version) o

        if (id != version.id) return false
        if (major != version.major) return false
        if (minor != version.minor) return false
        if (revision != version.revision) return false
        if (snapshot != version.snapshot) return false
        if (suffix != version.suffix) return false

        return true
    }

    int hashCode() {
        int result
        result = major
        result = 31 * result + minor
        result = 31 * result + revision
        result = 31 * result + id
        result = 31 * result + (snapshot ? 1 : 0)
        result = 31 * result + (suffix != null ? suffix.hashCode() : 0)
        return result
    }
}
