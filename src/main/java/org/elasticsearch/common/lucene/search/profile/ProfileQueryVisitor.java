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

package org.elasticsearch.common.lucene.search.profile;

import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.lucene.search.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * This class walks a query and wraps all applicable components in ProfileQuery
 * or ProfileFilter
 */
public class ProfileQueryVisitor extends Visitor<Object, ProfileComponent> {

    private ProfileQuery parent = null;

    public ProfileQueryVisitor(ProfileQuery parent) {
        this();
        this.parent = parent;
    }

    public ProfileQueryVisitor() {
        super(ProfileQueryVisitor.class, Object.class, ProfileComponent.class, new InvocationDispatcher.Disambiguator() {
            @Override
            public Method disambiguate(Class<?> dispatchableType, Class<?> parameterType, List<Method> methods, String methodName) {
                ListIterator<Method> iter = methods.listIterator();

                Method genericQuery = null;
                while(iter.hasNext()){
                    Method next = iter.next();
                    if(next.getParameterTypes()[0].equals(Query.class)){
                        genericQuery = next;
                        iter.remove();
                    }
                }

                // If there is only one method left, we found a (hopefully) more specific method
                if (methods.size() == 1) {
                    return methods.get(0);
                } else if (methods.size() > 1 && genericQuery != null) {
                    // If there are still multiple options, try to fallback to the most basic Query method call
                    // Wont be great, but might muddle through
                    return genericQuery;
                } else {
                    // Otherwise just give up
                    throw new InvocationDispatcher.AmbigousMethodException(dispatchableType, parameterType, methods, methodName);
                }

            }
        });
    }

    public ProfileQuery visit(BooleanQuery booleanQuery) {

        ProfileQuery newProfile = new ProfileQuery();

        // TODO replace this later with in-place updates
        BooleanQuery newQuery = new BooleanQuery(booleanQuery.isCoordDisabled());

        for (BooleanClause clause : booleanQuery.clauses()) {
            ProfileQuery pQuery = (ProfileQuery) apply(clause.getQuery());
            pQuery.setParentQuery(parent);
            newQuery.add(pQuery, clause.getOccur());
        }
        newQuery.setBoost(booleanQuery.getBoost());
        newQuery.setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());

        newProfile.setSubQuery(newQuery);
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileQuery visit(XFilteredQuery query) {
        ProfileQuery newProfile = new ProfileQuery();

        ProfileQuery pQuery = (ProfileQuery) apply(query.getQuery());
        pQuery.setParentQuery(parent);

        ProfileFilter pFilter = (ProfileFilter) apply(query.getFilter());
        pFilter.setParentQuery(parent);

        XFilteredQuery newQuery = new XFilteredQuery(pQuery, pFilter);
        newQuery.setBoost(query.getBoost());

        newProfile.setSubQuery(newQuery);
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileQuery visit(XConstantScoreQuery query) {

        ProfileQuery newProfile = new ProfileQuery();

        ProfileFilter pFilter = (ProfileFilter) apply(query.getFilter());
        pFilter.setParentQuery(parent);

        XConstantScoreQuery newQuery = new XConstantScoreQuery(pFilter);
        newQuery.setBoost(query.getBoost());

        newProfile.setSubQuery(newQuery);
        newProfile.setParentQuery(parent);
        return newProfile;
    }


    public ProfileQuery visit(ConstantScoreQuery query) {

        ProfileQuery newProfile = new ProfileQuery();
        Query q = query.getQuery();

        if (q != null) {
            ProfileQuery pQuery = (ProfileQuery) apply(q);
            pQuery.setParentQuery(parent);
            ConstantScoreQuery newQuery = new ConstantScoreQuery(pQuery);
            newQuery.setBoost(query.getBoost());
            newProfile.setSubQuery(newQuery);
        } else {
            ProfileFilter pFilter = (ProfileFilter) apply(query.getFilter());
            pFilter.setParentQuery(parent);
            ConstantScoreQuery newQuery = new ConstantScoreQuery(pFilter);
            newQuery.setBoost(query.getBoost());
            newProfile.setSubQuery(newQuery);
        }

        newProfile.setParentQuery(parent);
        return newProfile;

    }

    public ProfileQuery visit(DisjunctionMaxQuery query) {
        ProfileQuery newProfile = new ProfileQuery();
        DisjunctionMaxQuery newDis = new DisjunctionMaxQuery(query.getTieBreakerMultiplier());

        for (Query disjunct : query.getDisjuncts()) {
            ProfileQuery pQuery = (ProfileQuery) apply(disjunct);
            pQuery.setParentQuery(parent);
            newDis.add(pQuery);
        }

        newDis.setBoost(query.getBoost());
        newProfile.setSubQuery(newDis);
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileFilter visit(XBooleanFilter boolFilter) {

        XBooleanFilter newFilter = new XBooleanFilter();

        for (FilterClause clause : boolFilter.clauses()) {
            ProfileFilter pFilter = (ProfileFilter) apply(clause.getFilter());
            pFilter.setParentQuery(parent);
            newFilter.add(pFilter, clause.getOccur());
        }

        ProfileFilter newProfile = new ProfileFilter(newFilter);
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileFilter visit(AndFilter filter) {
        ArrayList<ProfileFilter> pFilters = new ArrayList<ProfileFilter>(filter.filters().size());
        for (Filter f : filter.filters()) {
            ProfileFilter pf = (ProfileFilter)apply(f);
            pf.setParentQuery(parent);
            pFilters.add(pf);
        }
        ProfileFilter newProfile = new ProfileFilter(new AndFilter(pFilters));
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileFilter visit(OrFilter filter) {
        ArrayList<ProfileFilter> pFilters = new ArrayList<ProfileFilter>(filter.filters().size());
        for (Filter f : filter.filters()) {
            ProfileFilter pf = (ProfileFilter)apply(f);
            pf.setParentQuery(parent);
        }
        ProfileFilter newProfile = new ProfileFilter(new OrFilter(pFilters));
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileFilter visit(NotFilter filter) {
        ProfileFilter pf = (ProfileFilter)apply(filter.filter());
        pf.setParentQuery(parent);

        NotFilter newNot = new NotFilter(pf);
        ProfileFilter newProfile = new ProfileFilter(newNot);

        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileQuery visit(ToParentBlockJoinQuery query) throws NoSuchFieldException, IllegalAccessException {
        Field origChildQueryField = query.getClass().getDeclaredField("origChildQuery");
        origChildQueryField.setAccessible(true);

        Field parentsFilterField = query.getClass().getDeclaredField("parentsFilter");
        parentsFilterField.setAccessible(true);

        Field scoreModeField = query.getClass().getDeclaredField("scoreMode");
        scoreModeField.setAccessible(true);

        ProfileQuery innerQuery = (ProfileQuery) apply(origChildQueryField.get(query));
        ProfileFilter parentsFilter = (ProfileFilter) apply(parentsFilterField.get(query));
        ScoreMode scoreMode = (ScoreMode) scoreModeField.get(query);

        ToParentBlockJoinQuery newQuery = new ToParentBlockJoinQuery(innerQuery, parentsFilter, scoreMode);
        newQuery.setBoost(query.getBoost());

        ProfileQuery newProfile = new ProfileQuery(newQuery);
        newProfile.setParentQuery(parent);

        return newProfile;
    }

    public ProfileQuery visit(Query query) {
        ProfileQuery newProfile =  new ProfileQuery(query);
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileFilter visit(Filter filter) {
        ProfileFilter newProfile =  new ProfileFilter(filter);
        newProfile.setParentQuery(parent);
        return newProfile;
    }

    public ProfileQuery visit(ProfileQuery query) {
        return query;
    }
}
