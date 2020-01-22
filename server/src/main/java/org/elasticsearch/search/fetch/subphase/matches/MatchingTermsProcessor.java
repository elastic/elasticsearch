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

package org.elasticsearch.search.fetch.subphase.matches;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.NamedQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MatchingTermsProcessor implements MatchesProcessor {

    public static final String NAME = "matching_terms";

    @Override
    public MatchesResult process(Matches matches, Settings settings) throws IOException {

        Map<String, List<String>> matchingTerms = matchingTerms(matches);

        Map<String, Map<String, List<String>>> perQuery = new HashMap<>();
        if (settings.getAsBoolean("named_queries", false)) {
            for (NamedQuery.NamedMatches m : NamedQuery.findNamedMatches(matches)) {
                perQuery.put(m.getName(), matchingTerms(m));
            }
        }

        return new MatchingTerms(matchingTerms, perQuery);
    }

    private static Map<String, List<String>> matchingTerms(Matches matches) throws IOException {
        Map<String, List<String>> matchingTerms = new HashMap<>();
        for (Matches leaf : findLeaves(matches)) {
            for (Term term : findTerms(leaf)) {
                matchingTerms.compute(term.field(), (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(term.text());
                    return v;
                });
            }
        }
        return matchingTerms;
    }

    private static List<Matches> findLeaves(Matches matches) {
        List<Matches> leaves = new ArrayList<>();
        List<Matches> toProcess = new LinkedList<>();
        toProcess.add(matches);

        while (toProcess.isEmpty() == false) {
            Matches m = toProcess.remove(0);
            Collection<Matches> sub = m.getSubMatches();
            if (sub.isEmpty()) {
                leaves.add(m);
            } else {
                toProcess.addAll(sub);
            }
        }

        return leaves;
    }

    private static Set<Term> findTerms(Matches matches) throws IOException {
        Set<Term> terms = new HashSet<>();
        for (String field : matches) {
            MatchesIterator it = matches.getMatches(field);
            it.next();
            it.getQuery().visit(QueryVisitor.termCollector(terms));
        }
        return terms;
    }

}
