package org.elasticsearch.common.lucene.search.profile;


import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.XFilteredQuery;

/**
 * This class walks a query and wraps all applicable queries in ProfileQuery queries
 */
public class ProfileQueryVisitor extends Visitor<Query, ProfileQuery> {

    public ProfileQueryVisitor() {
        super(ProfileQueryVisitor.class, Query.class, ProfileQuery.class);
    }

    public ProfileQuery visit(BooleanQuery booleanQuery) {

        // TODO replace this later with in-place updates
        BooleanQuery newQuery = new BooleanQuery(booleanQuery.isCoordDisabled());

        for (BooleanClause clause : booleanQuery.clauses()) {
            ProfileQuery pQuery = apply(clause.getQuery());
            newQuery.add(pQuery, clause.getOccur());
        }

        return new ProfileQuery(newQuery);
    }

    public ProfileQuery visit(XFilteredQuery query) {
        XFilteredQuery newQuery = new XFilteredQuery(apply(query.getQuery()), query.getFilter());
        return new ProfileQuery(newQuery);
    }

    public ProfileQuery visit(Query query) {
        return new ProfileQuery(query);
    }
}
