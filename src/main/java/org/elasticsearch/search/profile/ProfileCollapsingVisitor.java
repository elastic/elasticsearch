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

package org.elasticsearch.search.profile;

import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.lucene.search.*;
import org.elasticsearch.common.lucene.search.profile.ProfileFilter;
import org.elasticsearch.common.lucene.search.profile.ProfileQuery;
import org.elasticsearch.common.lucene.search.profile.Visitor;

import java.lang.reflect.Field;
import java.util.ArrayList;


/**
 * This Visitor will take an input Query (presumably one that contains one or
 * more ProfileQuery/ProfileFilter) and return a single Profile Object.
 */
public class ProfileCollapsingVisitor extends Visitor<Object, ArrayList> {

    public ProfileCollapsingVisitor() {
        super(ProfileCollapsingVisitor.class, Object.class, ArrayList.class);
    }

    public ArrayList<Profile> visit(BooleanQuery booleanQuery) {

        ArrayList<Profile> profiles = new ArrayList<Profile>();

        for (BooleanClause clause : booleanQuery.clauses()) {
            ArrayList<Profile> tProfiles = apply(clause.getQuery());
            if (tProfiles != null && tProfiles.size() > 0) {
                profiles.addAll(tProfiles);
            }
        }

        return profiles;
    }

    public ArrayList<Profile> visit(DisjunctionMaxQuery disMaxQuery) {

        ArrayList<Profile> profiles = new ArrayList<Profile>();

        for (Query disjunct : disMaxQuery.getDisjuncts()) {
            ArrayList<Profile> tProfiles = apply(disjunct);
            if (tProfiles != null && tProfiles.size() > 0) {
                profiles.addAll(tProfiles);
            }
        }

        return profiles;
    }

    public ArrayList<Profile> visit(ProfileQuery pQuery) {
        Profile p =  new Profile();
        p.setClassName(pQuery.className());
        p.setDetails(pQuery.details());

        return this.compileResults(p, apply(pQuery.subQuery()), pQuery.time());
    }

    public ArrayList<Profile> visit(ProfileFilter pFilter) {
        Profile p =  new Profile();
        p.setClassName(pFilter.className());
        p.setDetails(pFilter.details());

        return this.compileResults(p, apply(pFilter.subFilter()), pFilter.time());
    }

    public ArrayList<Profile> visit(XFilteredQuery query) {
        ArrayList<Profile> profiles = apply(query.getQuery());
        ArrayList<Profile> pFilters = apply(query.getFilter());

        if (pFilters != null) {
            profiles.addAll(pFilters);
        }

       return profiles;
    }

    public ArrayList<Profile> visit(XConstantScoreQuery query) {
        Query q = query.getQuery();
        Filter f = query.getFilter();

        ArrayList<Profile> profiles = new ArrayList<>();

        if (q != null) {
            ArrayList<Profile> pQuery = apply(query.getQuery());
            if (pQuery != null) {
                profiles.addAll(pQuery);
            }
        }

        if (f != null) {
            ArrayList<Profile> pFilters = apply(query.getFilter());
            if (pFilters != null) {
                profiles.addAll(pFilters);
            }
        }

        return profiles;
    }

    public ArrayList<Profile> visit(ConstantScoreQuery query) {
        Query q = query.getQuery();
        ArrayList<Profile> profiles;

        if (q != null) {
           profiles = apply(query.getQuery());
        } else {
           profiles = apply(query.getFilter());
        }

        return profiles;
    }

    public ArrayList<Profile> visit(XBooleanFilter filter) {
        ArrayList<Profile> profiles = new ArrayList<Profile>();

        for (FilterClause clause : filter.clauses()) {
            ArrayList<Profile> tProfiles = apply(clause.getFilter());
            if (tProfiles != null && tProfiles.size() > 0) {
                profiles.addAll(tProfiles);
            }
        }

        return profiles;

    }

    public ArrayList<Profile> visit(AndFilter filter) {
        ArrayList<Profile> profiles = new ArrayList<Profile>();

        for (Filter clause : filter.filters()) {
            ArrayList<Profile> tProfiles = apply(clause);
            if (tProfiles != null && tProfiles.size() > 0) {
                profiles.addAll(tProfiles);
            }
        }

        return profiles;
    }

    public ArrayList<Profile> visit(OrFilter filter) {
        ArrayList<Profile> profiles = new ArrayList<Profile>();

        for (Filter clause : filter.filters()) {
            ArrayList<Profile> tProfiles = apply(clause);
            if (tProfiles != null && tProfiles.size() > 0) {
                profiles.addAll(tProfiles);
            }
        }

        return profiles;
    }

    public ArrayList<Profile> visit(ToParentBlockJoinQuery query) throws NoSuchFieldException, IllegalAccessException {
        ArrayList<Profile> profiles = new ArrayList<Profile>();

        Field origChildQueryField = query.getClass().getDeclaredField("origChildQuery");
        origChildQueryField.setAccessible(true);

        Field parentsFilterField = query.getClass().getDeclaredField("parentsFilter");
        parentsFilterField.setAccessible(true);

        profiles.addAll(apply(origChildQueryField.get(query)));
        profiles.addAll(apply(parentsFilterField.get(query)));

        return profiles;
    }

    public ArrayList<Profile> visit(NotFilter filter) {
        return apply(filter.filter());
    }

    public ArrayList<Profile> visit(Query query) {
        return null;
    }

    public ArrayList<Profile> visit(Filter filter) {
        return null;
    }

    /**
     * Utility method to merge the timings of an array of components into the parent Profile
     */
    private ArrayList<Profile> compileResults(Profile p, ArrayList<Profile> components, long extraTime) {
        long time = 0;

        if (components != null && components.size() != 0) {
            for (Profile component : components) {
                time += component.time();
                p.addComponent(component);
            }
        }
        time += extraTime;
        p.setTime(time);

        ArrayList<Profile> pList = new ArrayList<Profile>(1);
        pList.add(p);

        return pList;
    }

}
