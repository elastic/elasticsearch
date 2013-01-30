/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.rescore;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import com.google.common.collect.ImmutableMap;

/**
 */
public class RescorePhase extends AbstractComponent implements SearchPhase {
    
    @Inject
    public RescorePhase(Settings settings) {
        super(settings);
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("rescore", new RescoreParseElement());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) throws ElasticSearchException {
        final RescoreSearchContext ctx = context.rescore();
        final Rescorer rescorer = ctx.rescorer();
        try {
            rescorer.rescore(context.queryResult().topDocs(), context, ctx);
        } catch (IOException e) {
            throw new ElasticSearchException("Rescore Phase Failed", e);
        }    
    }
  
    

    

}
