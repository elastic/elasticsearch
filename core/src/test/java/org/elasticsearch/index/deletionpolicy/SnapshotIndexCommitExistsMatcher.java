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

package org.elasticsearch.index.deletionpolicy;

import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 *
 */
public class SnapshotIndexCommitExistsMatcher extends TypeSafeMatcher<SnapshotIndexCommit> {

    @Override
    public boolean matchesSafely(SnapshotIndexCommit snapshotIndexCommit) {
        try {
            HashSet<String> files = Sets.newHashSet(snapshotIndexCommit.getDirectory().listAll());
            for (String fileName : snapshotIndexCommit.getFiles()) {
                if (files.contains(fileName) == false) {
                    return false;
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("an index commit existence");
    }

    public static Matcher<SnapshotIndexCommit> snapshotIndexCommitExists() {
        return new SnapshotIndexCommitExistsMatcher();
    }
}
