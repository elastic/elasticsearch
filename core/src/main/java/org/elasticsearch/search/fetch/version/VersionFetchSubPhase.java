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
package org.elasticsearch.search.fetch.version;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class VersionFetchSubPhase implements FetchSubPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("version", new VersionParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.version();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        // it might make sense to cache the TermDocs on a shared fetch context and just skip here)
        // it is going to mean we work on the high level multi reader and not the lower level reader as is
        // the case below...
        long version;
        try {
            BytesRef uid = Uid.createUidAsBytes(hitContext.hit().type(), hitContext.hit().id());
            version = Versions.loadVersion(
                    hitContext.readerContext().reader(),
                    new Term(UidFieldMapper.NAME, uid)
            );
        } catch (IOException e) {
            throw new ElasticsearchException("Could not query index for _version", e);
        }

        if (version < 0) {
            version = -1;
        }
        hitContext.hit().version(version);
    }
}
