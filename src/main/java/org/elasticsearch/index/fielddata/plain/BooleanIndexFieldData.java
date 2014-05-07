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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;

public class BooleanIndexFieldData extends PagedBytesIndexFieldData {

    private static final BytesRef TRUE = new BytesRef(Boolean.TRUE.toString());
    private static final BytesRef FALSE = new BytesRef(Boolean.FALSE.toString());

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<PagedBytesAtomicFieldData> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                                               IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService,
                                                               GlobalOrdinalsBuilder globalOrdinalBuilder) {
            return new BooleanIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, breakerService, globalOrdinalBuilder);
        }
    }

    private BooleanIndexFieldData(Index index, Settings indexSettings, Names fieldNames, FieldDataType fieldDataType,
            IndexFieldDataCache cache, CircuitBreakerService breakerService, GlobalOrdinalsBuilder globalOrdinalsBuilder) {
        super(index, indexSettings, fieldNames, fieldDataType, cache, breakerService, globalOrdinalsBuilder);
    }

    @Override
    protected TermsEnum wrapTermsEnum(TermsEnum in) {
        return new FilterAtomicReader.FilterTermsEnum(in) {

            @Override
            public BytesRef next() throws IOException {
                final BytesRef next = in.next();
                if (next == null) {
                    return null;
                } else if (next.equals(BooleanFieldMapper.Values.FALSE)) {
                    return FALSE;
                } else if (next.equals(BooleanFieldMapper.Values.TRUE)) {
                    return TRUE;
                } else {
                    assert false : "Unexpected term: " + next;
                    return next;
                }
            }

        };
    }

}
