package org.elasticsearch.search.profile;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.search.profile.ProfileQuery;
import org.elasticsearch.common.lucene.search.profile.Visitor;


/**
 * This Visitor will take an input Query (presumably one that contains one or more ProfileQuery)
 * and return a single Profile Object.
 */
public class ProfileCollapsingVisitor extends Visitor<Query, Profile> {

    public ProfileCollapsingVisitor() {
        super(ProfileCollapsingVisitor.class, Query.class, Profile.class);
    }

    public Profile visit(BooleanQuery booleanQuery) {

        Profile p = new Profile();
        p.setClassName(booleanQuery.getClass().getSimpleName());
        p.setDetails(booleanQuery.toString());

        long time = 0;

        for (BooleanClause clause : booleanQuery.clauses()) {
            Profile component = apply(clause.getQuery());
            time += component.time();
            p.addComponent(component);
        }
        p.setTime(time);

        return p;
    }

    public Profile visit(ProfileQuery pQuery) {
        Profile p =  new Profile();
        p.setClassName(pQuery.subQuery().getClass().getSimpleName());
        p.setDetails(pQuery.subQuery().toString());

        Profile component = apply(pQuery.subQuery());
        if (component != null) {

            // TODO this is a hack to make the output nicer...needs to be fixed!
            // Basically circumvents the visitor pattern because Bools (and other compound)
            // will show twice in output
            if (pQuery.subQuery() instanceof BooleanQuery) {
                p = component;
            } else {
                p.setTime(pQuery.time() + component.time());
                p.addComponent(component);
            }

        } else {
            p.setTime(pQuery.time());
        }

        return p;
    }

    public Profile visit(XFilteredQuery query) {
        return apply(query.getQuery());
    }

    public Profile visit(Query query) {
        return null;
    }
}
