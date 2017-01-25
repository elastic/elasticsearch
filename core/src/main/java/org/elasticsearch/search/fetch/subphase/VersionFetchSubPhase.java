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
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public final class VersionFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.version() == false ||
            (context.storedFieldsContext() != null && context.storedFieldsContext().fetchFields() == false)) {
            return;
        }
        long version = Versions.NOT_FOUND;
        try {
            NumericDocValues versions = hitContext.reader().getNumericDocValues(VersionFieldMapper.NAME);
            if (versions != null) {
                version = versions.get(hitContext.docId());
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Could not retrieve version", e);
        }
        hitContext.hit().version(version < 0 ? -1 : version);
    }
}
