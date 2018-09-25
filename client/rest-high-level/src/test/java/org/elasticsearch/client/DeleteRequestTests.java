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

package org.elasticsearch.client;

import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Optional;

public class DeleteRequestTests extends ESTestCase {

    public void testValidateDefaults() {
        DeleteRequest request = new DeleteRequest("index", "id");
        assertEquals(Optional.empty(), request.validate());
    }

    public void testValidateVersion() {
        DeleteRequest request = new DeleteRequest("index", "id")
                .setVersion(4L);
        assertEquals(Optional.empty(), request.validate());

        request.setVersion(Versions.NOT_FOUND);
        Optional<ValidationException> exception = request.validate();
        assertTrue(exception.isPresent());
        assertEquals(
                Collections.singletonList("illegal version value [-1] for version type [INTERNAL]"),
                exception.get().validationErrors());
    }

    public void testValidateVersionType() {
        DeleteRequest request = new DeleteRequest("index", "id")
                .setVersionType(VersionType.EXTERNAL)
                .setVersion(3L);
        assertEquals(Optional.empty(), request.validate());

        request.setVersionType(VersionType.FORCE);
        Optional<ValidationException> exception = request.validate();
        assertTrue(exception.isPresent());
        assertEquals(
                Collections.singletonList("version type [force] may no longer be used"),
                exception.get().validationErrors());
    }

    public void testEmptyRoutingIsTranslatedToNull() {
        DeleteRequest request = new DeleteRequest("index", "id");
        assertNull(request.getRouting());
        request.setRouting("");
        assertNull(request.getRouting());
        request.setRouting("a");
        assertEquals("a", request.getRouting());
    }

}
