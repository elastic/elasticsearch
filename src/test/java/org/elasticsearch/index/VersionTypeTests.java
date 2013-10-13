package org.elasticsearch.index;
/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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


import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class VersionTypeTests extends ElasticsearchTestCase {
    @Test
    public void testInternalVersionConflict() throws Exception {

        assertFalse(VersionType.INTERNAL.isVersionConflict(10, Versions.MATCH_ANY));
        // if we don't have a version in the index we accept everything
        assertFalse(VersionType.INTERNAL.isVersionConflict(Versions.NOT_SET, 10));
        assertFalse(VersionType.INTERNAL.isVersionConflict(Versions.NOT_SET, Versions.MATCH_ANY));

        // if we didn't find a version (but the index does support it), we don't like it unless MATCH_ANY
        assertTrue(VersionType.INTERNAL.isVersionConflict(Versions.NOT_FOUND, Versions.NOT_FOUND));
        assertTrue(VersionType.INTERNAL.isVersionConflict(Versions.NOT_FOUND, 10));
        assertFalse(VersionType.INTERNAL.isVersionConflict(Versions.NOT_FOUND, Versions.MATCH_ANY));

        // and the stupid usual case
        assertFalse(VersionType.INTERNAL.isVersionConflict(10, 10));
        assertTrue(VersionType.INTERNAL.isVersionConflict(9, 10));
        assertTrue(VersionType.INTERNAL.isVersionConflict(10, 9));

// Old indexing code, dictating behavior
//        if (expectedVersion != Versions.MATCH_ANY && currentVersion != Versions.NOT_SET) {
//            // an explicit version is provided, see if there is a conflict
//            // if we did not find anything, and a version is provided, so we do expect to find a doc under that version
//            // this is important, since we don't allow to preset a version in order to handle deletes
//            if (currentVersion == Versions.NOT_FOUND) {
//                throw new VersionConflictEngineException(shardId, index.type(), index.id(), Versions.NOT_FOUND, expectedVersion);
//            } else if (expectedVersion != currentVersion) {
//                throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
//            }
//        }
//        updatedVersion = (currentVersion == Versions.NOT_SET || currentVersion == Versions.NOT_FOUND) ? 1 : currentVersion + 1;
    }

    @Test
    public void testExternalVersionConflict() throws Exception {

        assertFalse(VersionType.EXTERNAL.isVersionConflict(Versions.NOT_FOUND, 10));
        assertFalse(VersionType.EXTERNAL.isVersionConflict(Versions.NOT_SET, 10));
        // MATCH_ANY must throw an exception in the case of external version, as the version must be set! it used as the new value
        assertTrue(VersionType.EXTERNAL.isVersionConflict(10, Versions.MATCH_ANY));

        // if we didn't find a version (but the index does support it), we always accept
        assertFalse(VersionType.EXTERNAL.isVersionConflict(Versions.NOT_FOUND, Versions.NOT_FOUND));
        assertFalse(VersionType.EXTERNAL.isVersionConflict(Versions.NOT_FOUND, 10));
        assertFalse(VersionType.EXTERNAL.isVersionConflict(Versions.NOT_FOUND, Versions.MATCH_ANY));

        // and the standard behavior
        assertTrue(VersionType.EXTERNAL.isVersionConflict(10, 10));
        assertFalse(VersionType.EXTERNAL.isVersionConflict(9, 10));
        assertTrue(VersionType.EXTERNAL.isVersionConflict(10, 9));


// Old indexing code, dictating behavior
//        // an external version is provided, just check, if a local version exists, that its higher than it
//        // the actual version checking is one in an external system, and we just want to not index older versions
//        if (currentVersion >= 0) { // we can check!, its there
//            if (currentVersion >= index.version()) {
//                throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, index.version());
//            }
//        }
//        updatedVersion = index.version();
    }


    @Test
    public void testUpdateVersion() {

        assertThat(VersionType.INTERNAL.updateVersion(Versions.NOT_SET, 10), equalTo(1l));
        assertThat(VersionType.INTERNAL.updateVersion(Versions.NOT_FOUND, 10), equalTo(1l));
        assertThat(VersionType.INTERNAL.updateVersion(1, 1), equalTo(2l));
        assertThat(VersionType.INTERNAL.updateVersion(2, Versions.MATCH_ANY), equalTo(3l));


        assertThat(VersionType.EXTERNAL.updateVersion(Versions.NOT_SET, 10), equalTo(10l));
        assertThat(VersionType.EXTERNAL.updateVersion(Versions.NOT_FOUND, 10), equalTo(10l));
        assertThat(VersionType.EXTERNAL.updateVersion(1, 10), equalTo(10l));

// Old indexing code
//        if (index.versionType() == VersionType.INTERNAL) { // internal version type
//            updatedVersion = (currentVersion == Versions.NOT_SET || currentVersion == Versions.NOT_FOUND) ? 1 : currentVersion + 1;
//        } else { // external version type
//            updatedVersion = expectedVersion;
//        }
    }
}
