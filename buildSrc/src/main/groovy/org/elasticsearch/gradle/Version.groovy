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

    public Version(int major, int minor, int bugfix, boolean snapshot) {
        this.major = major
        this.minor = minor
        this.bugfix = bugfix
        this.snapshot = snapshot
        this.id = major * 100000 + minor * 1000 + bugfix * 10 +
            (snapshot ? 1 : 0)
    }

    public static Version fromString(String s) {
        String[] parts = s.split('\\.')
        String bugfix = parts[2]
        boolean snapshot = false
        if (bugfix.contains('-')) {
            snapshot = bugfix.endsWith('-SNAPSHOT')
            bugfix = bugfix.split('-')[0]
        }
        return new Version(parts[0] as int, parts[1] as int, bugfix as int,
            snapshot)
    }

    @Override
    public String toString() {
        String snapshotStr = snapshot ? '-SNAPSHOT' : ''
        return "${major}.${minor}.${bugfix}${snapshotStr}"
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
