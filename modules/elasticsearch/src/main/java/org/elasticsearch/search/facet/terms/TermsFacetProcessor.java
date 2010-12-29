/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.terms;

import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.TObjectIntHashMap;
import org.elasticsearch.common.trove.TObjectIntIterator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class TermsFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject public TermsFacetProcessor(Settings settings) {
        super(settings);
        InternalFacet.Streams.registerStream(InternalTermsFacet.STREAM, InternalTermsFacet.TYPE);
    }

    @Override public String[] types() {
        return new String[]{TermsFacet.TYPE};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;
        int size = 10;

        String[] fieldsNames = null;
        ImmutableSet<String> excluded = ImmutableSet.of();
        String regex = null;
        String regexFlags = null;
        TermsFacet.ComparatorType comparatorType = TermsFacet.ComparatorType.COUNT;
        String scriptLang = null;
        String script = null;
        Map<String, Object> params = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    params = parser.map();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("exclude".equals(currentFieldName)) {
                    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.add(parser.text());
                    }
                    excluded = builder.build();
                } else if ("fields".equals(currentFieldName)) {
                    List<String> fields = Lists.newArrayListWithCapacity(4);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                    fieldsNames = fields.toArray(new String[fields.size()]);
                }
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script_field".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("regex".equals(currentFieldName)) {
                    regex = parser.text();
                } else if ("regex_flags".equals(currentFieldName) || "regexFlags".equals(currentFieldName)) {
                    regexFlags = parser.text();
                } else if ("order".equals(currentFieldName) || "comparator".equals(field)) {
                    comparatorType = TermsFacet.ComparatorType.fromString(parser.text());
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if ("_index".equals(field)) {
            return new IndexNameFacetCollector(facetName, context.shardTarget().index(), comparatorType, size);
        }

        Pattern pattern = null;
        if (regex != null) {
            pattern = Regex.compile(regex, regexFlags);
        }
        if (fieldsNames != null) {
            return new TermsFieldsFacetCollector(facetName, fieldsNames, size, comparatorType, context, excluded, pattern, scriptLang, script, params);
        }
        if (field == null && fieldsNames == null && script != null) {
            return new TermsScriptFieldFacetCollector(facetName, size, comparatorType, context, excluded, pattern, scriptLang, script, params);
        }
        return new TermsFacetCollector(facetName, field, size, comparatorType, context, excluded, pattern, scriptLang, script, params);
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalTermsFacet first = (InternalTermsFacet) facets.get(0);
        TObjectIntHashMap<String> aggregated = aggregateCache.get().get();
        aggregated.clear();

        for (Facet facet : facets) {
            TermsFacet mFacet = (TermsFacet) facet;
            for (TermsFacet.Entry entry : mFacet) {
                aggregated.adjustOrPutValue(entry.term(), entry.count(), entry.count());
            }
        }

        BoundedTreeSet<TermsFacet.Entry> ordered = new BoundedTreeSet<TermsFacet.Entry>(first.comparatorType().comparator(), first.requiredSize);
        for (TObjectIntIterator<String> it = aggregated.iterator(); it.hasNext();) {
            it.advance();
            ordered.add(new TermsFacet.Entry(it.key(), it.value()));
        }
        first.entries = ordered;
        return first;
    }

    private static ThreadLocal<ThreadLocals.CleanableValue<TObjectIntHashMap<String>>> aggregateCache = new ThreadLocal<ThreadLocals.CleanableValue<TObjectIntHashMap<String>>>() {
        @Override protected ThreadLocals.CleanableValue<TObjectIntHashMap<String>> initialValue() {
            return new ThreadLocals.CleanableValue<TObjectIntHashMap<String>>(new TObjectIntHashMap<String>());
        }
    };
}
