/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.tests.codecs.asserting.AssertingPointsFormat;
import org.elasticsearch.index.codec.ElasticsearchCodec;

class AssertingFormatWrapper extends ElasticsearchCodec.FormatWrapper {
    /*public PostingsFormat wrap(PostingsFormat format) { return new AssertingPostingsFormat(format); }
    public DocValuesFormat wrap(DocValuesFormat format) { return new AssertingDocValuesFormat(format); }
    public KnnVectorsFormat wrap(KnnVectorsFormat format) { return new AssertingKnnVectorsFormat(format); }
    public StoredFieldsFormat wrap(StoredFieldsFormat format) { return new AssertingStoredFieldsFormat(format); }*/
    public PointsFormat wrap(PointsFormat format) {
        return new AssertingPointsFormat(format);
    }
    /*public NormsFormat wrap(NormsFormat format) { return new AssertingNormsFormat(format); }
    public TermVectorsFormat wrap(TermVectorsFormat format) { return new AssertingTermVectorsFormat(format); }
    public LiveDocsFormat wrap(LiveDocsFormat format) { return new AssertingLiveDocsFormat(format); }*/
}
