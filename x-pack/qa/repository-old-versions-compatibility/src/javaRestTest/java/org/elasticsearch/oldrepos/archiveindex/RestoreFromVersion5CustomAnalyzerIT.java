/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.archiveindex;

import org.elasticsearch.oldrepos.Snapshot;
import org.elasticsearch.test.cluster.util.Version;

public class RestoreFromVersion5CustomAnalyzerIT extends ArchiveIndexTestCase {

    public RestoreFromVersion5CustomAnalyzerIT(Version version) {
        super(version, Snapshot.FIVE_CUSTOM_ANALYZER);
    }
}
