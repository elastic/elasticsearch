package org.elasticsearch.common.lucene.search.profile;


import org.apache.lucene.queryparser.surround.query.AndQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.XFilteredQuery;

/**
 * This class walks a query and wraps all applicable queries in ProfileQuery queries
 */
public class ProfileQueryVisitor extends Visitor<Object, ProfileComponent> {

    public ProfileQueryVisitor() {
        super(ProfileQueryVisitor.class, Object.class, ProfileComponent.class);
    }

    public ProfileQuery visit(BooleanQuery booleanQuery) {

        // TODO replace this later with in-place updates
        BooleanQuery newQuery = new BooleanQuery(booleanQuery.isCoordDisabled());

        for (BooleanClause clause : booleanQuery.clauses()) {
            ProfileQuery pQuery = (ProfileQuery) apply(clause.getQuery());
            newQuery.add(pQuery, clause.getOccur());
        }

        return new ProfileQuery(newQuery);
    }

    public ProfileQuery visit(XFilteredQuery query) {
        ProfileQuery pQuery = (ProfileQuery) apply(query.getQuery());
        ProfileFilter pFilter = (ProfileFilter) apply(query.getFilter());

        XFilteredQuery newQuery = new XFilteredQuery(pQuery, pFilter);
        return new ProfileQuery(newQuery);
    }

    public ProfileQuery visit(Query query) {
        return new ProfileQuery(query);
    }

    public ProfileFilter visit(Filter filter) {
        return new ProfileFilter(filter);
    }
}
