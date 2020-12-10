/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent.support.filtering;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;
import com.fasterxml.jackson.core.filter.TokenFilter;
import com.fasterxml.jackson.core.filter.TokenFilterContext;

import java.io.IOException;

/**
 * An extension of the Jockson {@link FilteringGeneratorDelegate} that preserves empty objects and
 * empty arrays in the filtered output. This is done by checking values during the close of the
 * array or object inside a custom token filter context.
 */
public class EmptyPreservingFilteringGeneratorDelegate extends FilteringGeneratorDelegate {

    public EmptyPreservingFilteringGeneratorDelegate(JsonGenerator generator,
                                                     TokenFilter filter,
                                                     boolean includePath,
                                                     boolean allowMultipleMatches
    ) {
        super(generator, filter, includePath, allowMultipleMatches);
        this._filterContext = new EmptyPreservingTokenFilterContext(EmptyPreservingTokenFilterContext.ROOT, null, filter, true);
    }

    static class EmptyPreservingTokenFilterContext extends TokenFilterContext {

        public static final int ROOT = JsonStreamContext.TYPE_ROOT;

        EmptyPreservingTokenFilterContext(int type, TokenFilterContext parent, TokenFilter filter, boolean startHandled) {
            super(type, parent, filter, startHandled);
        }

        @Override
        public TokenFilterContext createChildArrayContext(TokenFilter filter, boolean writeStart) {
            TokenFilterContext ctxt = _child;
            if (ctxt == null) {
                _child = ctxt = new EmptyPreservingTokenFilterContext(TYPE_ARRAY, this, filter, writeStart);
                return ctxt;
            }
            return ((EmptyPreservingTokenFilterContext) ctxt).reset(TYPE_ARRAY, filter, writeStart);
        }

        @Override
        public TokenFilterContext createChildObjectContext(TokenFilter filter, boolean writeStart) {
            TokenFilterContext ctxt = _child;
            if (ctxt == null) {
                _child = ctxt = new EmptyPreservingTokenFilterContext(TYPE_OBJECT, this, filter, writeStart);
                return ctxt;
            }
            return ((EmptyPreservingTokenFilterContext) ctxt).reset(TYPE_OBJECT, filter, writeStart);
        }

        @Override
        public TokenFilterContext closeArray(JsonGenerator gen) throws IOException {
            if (_startHandled) {
                gen.writeEndArray();
            }
            if ((_filter != null) && (_filter != TokenFilter.INCLUDE_ALL)) {
                if (_type == TYPE_ARRAY && _index == -1) { // empty
                    writePath(gen);
                    gen.writeEndArray();
                }
                _filter.filterFinishArray();
            }
            return _parent;
        }

        @Override
        public TokenFilterContext closeObject(JsonGenerator gen) throws IOException {
            if (_startHandled) {
                gen.writeEndObject();
            }
            if ((_filter != null) && (_filter != TokenFilter.INCLUDE_ALL)) {
                if (isStartHandled() == false && _child == null && hasCurrentName() == false) {
                    // empty!
                    _parent.writePath(gen);
                    gen.writeStartObject();
                    gen.writeEndObject();
                }
                _filter.filterFinishObject();
            }
            return _parent;
        }
    }
}
