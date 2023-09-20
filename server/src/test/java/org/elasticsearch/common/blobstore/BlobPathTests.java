/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.blobstore.BlobPath.Purpose;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class BlobPathTests extends ESTestCase {

    public void testBuildAsString() {
        BlobPath path = BlobPath.EMPTY;
        assertThat(path.buildAsString(), is(""));

        path = path.add("a");
        assertThat(path.buildAsString(), is("a/"));

        path = path.add("b").add("c");
        assertThat(path.buildAsString(), is("a/b/c/"));

        path = path.add("d/");
        assertThat(path.buildAsString(), is("a/b/c/d/"));
    }

    public void testPurpose() {
        // default purpose
        BlobPath path = BlobPath.EMPTY;
        assertThat(path.purpose(), is(Purpose.GENERIC));

        // purpose is propagated
        path = path.add("a");
        assertThat(path.purpose(), is(Purpose.GENERIC));

        // purpose can be updated
        final Purpose newPurpose = randomFrom(Purpose.values());
        path = path.add("b").purpose(newPurpose);
        if (randomBoolean()) {
            path = path.add("c");
        }
        assertThat(path.purpose(), is(newPurpose));
    }
}
