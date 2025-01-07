/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.archiveindex;

import org.elasticsearch.test.cluster.util.Version;

public class RestoreFromVersion5IT extends ArchiveIndexTestCase {

    public RestoreFromVersion5IT(Version version) {
        super(version);
    }

    public void testArchiveIndex() throws Exception {
        verifyCompatibility("5");
    }
}
