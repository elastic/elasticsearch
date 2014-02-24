package org.elasticsearch.search.profile;

import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.*;
import org.elasticsearch.common.lucene.search.profile.ProfileFilter;
import org.elasticsearch.common.lucene.search.profile.ProfileQuery;
import org.elasticsearch.common.lucene.search.profile.Visitor;
import sun.net.www.content.text.plain;

import java.util.ArrayList;


/**
 * This Visitor will take an input Query (presumably one that contains one or more ProfileQuery)
 * and return a single Profile Object.
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

    public ArrayList<Profile> visit(ProfileQuery pQuery) {
        Profile p =  new Profile();
        p.setClassName(pQuery.subQuery().getClass().getSimpleName());
        p.setDetails(pQuery.subQuery().toString());

        return this.compileResults(p, apply(pQuery.subQuery()), pQuery.time());
    }

    public ArrayList<Profile> visit(ProfileFilter pFilter) {
        Profile p =  new Profile();
        p.setClassName(pFilter.subFilter().getClass().getSimpleName());
        p.setDetails(pFilter.subFilter().toString());

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
        ArrayList<Profile> profiles = apply(query.getFilter());

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

    public ArrayList<Profile> visit(NotFilter filter) {
        return apply(filter);
    }

    public ArrayList<Profile> visit(Query query) {
        return null;
    }

    public ArrayList<Profile> visit(Filter filter) {
        return null;
    }

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
