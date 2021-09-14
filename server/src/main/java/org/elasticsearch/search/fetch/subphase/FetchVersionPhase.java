/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;

public final class FetchVersionPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.version() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            NumericDocValues versions = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                versions = readerContext.reader().getNumericDocValues(VersionFieldMapper.NAME);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                long version = Versions.NOT_FOUND;
                if (versions != null && versions.advanceExact(hitContext.docId())) {
                    version = versions.longValue();
                }
                hitContext.hit().version(version < 0 ? -1 : version);
            }
        };
    }
}
