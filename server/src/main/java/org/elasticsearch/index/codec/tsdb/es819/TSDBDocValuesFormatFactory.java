/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

public final class TSDBDocValuesFormatFactory {

    TSDBDocValuesFormatFactory() {}

    public static DocValuesFormat createDocValuesFormat(IndexVersion indexCreatedVersion, boolean useLargeBlockSize) {
        if (indexCreatedVersion.onOrAfter(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3)) {
            return new ES819Version3TSDBDocValuesFormat(useLargeBlockSize);
        } else {
            return ES819TSDBDocValuesFormat.getInstance(useLargeBlockSize);
        }
    }
}
